package org.embulk.filter.internal_http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.embulk.config.*;
import org.embulk.exec.ExecutionInterruptedException;
import org.embulk.spi.*;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class InternalHttpFilterPlugin implements FilterPlugin {
    private static final Logger logger = LoggerFactory.getLogger(InternalHttpFilterPlugin.class);

    private static final String defaultJavaTimestampFormat = "yyyy-MM-dd HH:mm:ss xxxx";
    private static final String defaultRubyTimestampFormat = "%Y-%m-%d %H:%M:%S %z";
    private static final String sampleDataModeColumnName0 = "data";
    private static final String sampleDataModeColumnName1 = "schema";
    private static final String requestJsonRootAttributeName = "rows";

    private static HashMap<String, TimestampParser> timestampParserMap = new HashMap<>();

    // NOTE: This is not spi.ColumnConfig
    public interface ColumnConfig extends Task {
        @Config("name")
        public String getName();

        @Config("type")
        public Type getType();

        @Config("format")
        @ConfigDefault("\"" + defaultRubyTimestampFormat + "\"")
        public String getFormat();
    }

    public interface PluginTask extends Task {
        @Config("url")
        @ConfigDefault("\"\"")
        public String getUrl();

        @Config("columns")
        @ConfigDefault("[]")
        public List<ColumnConfig> getColumns();

        @Config("sample_data_mode")
        @ConfigDefault("false")
        public boolean getSampleDataMode();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        validateConfig(task);
        Schema outputSchema = task.getSampleDataMode() ? buildSampleDataOutputSchema() : buildDefaultOutputSchema(task);
        initTimestampParserMap(task);

        control.run(task.dump(), outputSchema);
    }

    private void validateConfig(PluginTask task) {
        if (!task.getSampleDataMode()) {
            if (task.getUrl().isEmpty()) {
                throw new ConfigException("'url' is required, but not set.");
            }
            if (task.getColumns().isEmpty()) {
                throw new ConfigException("'columns' is required, but not set.");
            }
        }
    }

    private Schema buildDefaultOutputSchema(PluginTask task) {
        List<ColumnConfig> columns = task.getColumns();
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        for (ColumnConfig column : columns) {
            builder.add(new Column(i++, column.getName(), column.getType()));
        }
        return new Schema(builder.build());
    }

    private Schema buildSampleDataOutputSchema() {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        builder.add(new Column(0, sampleDataModeColumnName0, Types.JSON));
        builder.add(new Column(1, sampleDataModeColumnName1, Types.JSON));
        return new Schema(builder.build());
    }

    private void initTimestampParserMap(PluginTask task) {
        List<ColumnConfig> columns = task.getColumns();
        for (ColumnConfig column : columns) {
            if (column.getType().getName().equals("timestamp")) {
                timestampParserMap.put(column.getName(), TimestampParser.of(column.getFormat(), "UTC"));
            }
        }
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish() {
                pageBuilder.finish();
            }

            @Override
            public void close() {
                pageBuilder.close();
            }

            @Override
            public void add(Page page) {
                pageReader.setPage(page);

                ObjectMapper mapper = new ObjectMapper();

                ArrayNode schemaNode = mapper.createArrayNode();
                if (task.getSampleDataMode()) {
                    schemaNode = getSchemaNode();
                }

                ObjectNode requestRootNode = mapper.createObjectNode();
                ArrayNode requestPagesNode = mapper.createArrayNode();
                while (pageReader.nextRecord()) {
                    ObjectNode requestPageNode = mapper.createObjectNode();
                    for (Column column : inputSchema.getColumns()) {
                        if (pageReader.isNull(column)) {
                            requestPageNode.putNull(column.getName());
                            continue;
                        }
                        Type type = column.getType();
                        if (Types.STRING.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getString(column));
                        } else if (Types.DOUBLE.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getDouble(column));
                        } else if (Types.LONG.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getLong(column));
                        } else if (Types.BOOLEAN.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getBoolean(column));
                        } else if (Types.TIMESTAMP.equals(type)) {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(defaultJavaTimestampFormat).withZone(ZoneOffset.UTC);
                            requestPageNode.put(column.getName(), formatter.format(pageReader.getTimestamp(column).getInstant()));
                        } else if (Types.JSON.equals(type)) {
                            try {
                                requestPageNode.set(column.getName(), mapper.readTree(pageReader.getJson(column).toString()));
                            } catch (IOException e) {
                                logger.error(e.getMessage(), e);
                                throw new ExecutionInterruptedException(e);
                            }
                        }
                    }
                    if (task.getSampleDataMode()) {
                        try {
                            pageBuilder.setJson(new Column(0, sampleDataModeColumnName0, Types.JSON), new JsonParser().parse(mapper.writeValueAsString(requestPageNode)));
                            pageBuilder.setJson(new Column(1, sampleDataModeColumnName1, Types.JSON), new JsonParser().parse(mapper.writeValueAsString(schemaNode)));
                            pageBuilder.addRecord();
                        } catch (IOException e) {
                            logger.error(e.getMessage(), e);
                            throw new ExecutionInterruptedException(e);
                        }
                    }
                    requestPagesNode.add(requestPageNode);
                }
                requestRootNode.set(requestJsonRootAttributeName, requestPagesNode);

                if (task.getSampleDataMode()) {
                    return;
                }

                try {
                    String requestBody = mapper.writeValueAsString(requestRootNode);
                    // ref: https://stackoverflow.com/questions/31611861/why-setconnectionrequesttimeout-doesnt-stop-my-1-min-get-request
                    RequestConfig config = RequestConfig.custom()
                            .setSocketTimeout(5000)
                            .build();
                    try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
                        HttpPost httpPost = new HttpPost(task.getUrl());
                        StringEntity entity = new StringEntity(requestBody, "UTF-8");
                        httpPost.setEntity(entity);
                        try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
                            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                String responseBody = EntityUtils.toString(httpResponse.getEntity());
                                JsonNode responseRootNode = mapper.readTree(responseBody);
                                Iterator<JsonNode> responseRootIterator = responseRootNode.get(requestJsonRootAttributeName).elements();
                                while (responseRootIterator.hasNext()) {
                                    JsonNode responsePageNode = responseRootIterator.next();
                                    if (responsePageNode.isNull()) {
                                        continue;
                                    }
                                    for (Column column : outputSchema.getColumns()) {
                                        String name = column.getName();
                                        JsonNode val = responsePageNode.get(name);
                                        if (val.isNull()) {
                                            pageBuilder.setNull(column);
                                            continue;
                                        }
                                        Type type = column.getType();
                                        if (Types.STRING.equals(type)) {
                                            pageBuilder.setString(column, val.asText());
                                        } else if (Types.DOUBLE.equals(type)) {
                                            pageBuilder.setDouble(column, val.asDouble());
                                        } else if (Types.LONG.equals(type)) {
                                            pageBuilder.setLong(column, val.asLong());
                                        } else if (Types.BOOLEAN.equals(type)) {
                                            pageBuilder.setBoolean(column, val.asBoolean());
                                        } else if (Types.TIMESTAMP.equals(type)) {
                                            pageBuilder.setTimestamp(column, timestampParserMap.get(column.getName()).parse(val.asText()));
                                        } else if (Types.JSON.equals(type)) {
                                            pageBuilder.setJson(column, new JsonParser().parse(val.toString()));
                                        }
                                    }
                                    pageBuilder.addRecord();
                                }
                            } else {
                                logger.error("Internal API Error");
                                String responseBody = EntityUtils.toString(httpResponse.getEntity());
                                logger.error(responseBody);
                                throw new ExecutionInterruptedException(new Exception("Internal API Error"));
                            }
                        }
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    throw new ExecutionInterruptedException(e);
                }
            }

            private ArrayNode getSchemaNode() {
                ObjectMapper mapper = new ObjectMapper();
                ArrayNode schemaNode = mapper.createArrayNode();
                for (Column column : inputSchema.getColumns()) {
                    ArrayNode eachSchemaNode = mapper.createArrayNode();
                    eachSchemaNode.add(column.getName());
                    eachSchemaNode.add(column.getType().getName());
                    schemaNode.add(eachSchemaNode);
                }
                return schemaNode;
            }
        };
    }
}
