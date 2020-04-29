package org.embulk.filter.internal_http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.common.collect.ImmutableList;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.embulk.config.*;
import org.embulk.exec.ExecutionInterruptedException;
import org.embulk.spi.*;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class InternalHttpFilterPlugin
        implements FilterPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(InternalHttpFilterPlugin.class);

    // NOTE: This is not spi.ColumnConfig
    interface ColumnConfig extends Task
    {
        @Config("name")
        public String getName();

        @Config("type")
        public Type getType();
    }

    public interface PluginTask
            extends Task
    {
        @Config("url")
        public String getUrl();

        @Config("columns")
        @ConfigDefault("[]")
        public List<ColumnConfig> getColumns();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = buildOutputSchema(task, inputSchema);

        control.run(task.dump(), outputSchema);
    }

    private static Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        if (task.getColumns().isEmpty()) {
            return inputSchema;
        }

        List<ColumnConfig> columns = task.getColumns();
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        for (ColumnConfig column : columns) {
            Column outputColumn = new Column(i++, column.getName(), column.getType());
            builder.add(outputColumn);
        }
        return new Schema(builder.build());
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                ObjectMapper mapper = new ObjectMapper();

                ObjectNode requestRootNode = mapper.createObjectNode();
                ArrayNode requestPagesNode = mapper.createArrayNode();
                while (pageReader.nextRecord()) {
                    ObjectNode requestPageNode = mapper.createObjectNode();
                    for (Column column : inputSchema.getColumns()) {
                        Type type = column.getType();
                        if (Types.STRING.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getString(column));
                        }
                        else if (Types.DOUBLE.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getDouble(column));
                        }
                        else if (Types.LONG.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getLong(column));
                        }
                        else if (Types.BOOLEAN.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getBoolean(column));
                        }
                        else if (Types.TIMESTAMP.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getTimestamp(column).getEpochSecond());
                        }
                        else if (Types.JSON.equals(type)) {
                            requestPageNode.put(column.getName(), pageReader.getJson(column).toString());
                        }
                    }
                    requestPagesNode.add(requestPageNode);
                }
                requestRootNode.set("pages", requestPagesNode);

                try {
                    String requestBody = mapper.writeValueAsString(requestRootNode);
                    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                        HttpPost httpPost = new HttpPost(task.getUrl());
                        StringEntity entity = new StringEntity(requestBody, "UTF-8");
                        httpPost.setEntity(entity);
                        try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
                            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                String responseBody = EntityUtils.toString(httpResponse.getEntity());
                                JsonNode responseRootNode = mapper.readTree(responseBody);
                                Iterator<JsonNode> responseRootIterator = responseRootNode.get("pages").elements();
                                while (responseRootIterator.hasNext()) {
                                    JsonNode responsePageNode = responseRootIterator.next();
                                    for (Column column : outputSchema.getColumns()) {
                                        String name = column.getName();
                                        JsonNode val = responsePageNode.get(name);
                                        Type type = column.getType();
                                        if (Types.STRING.equals(type)) {
                                            pageBuilder.setString(column, val.asText());
                                        }
                                        else if (Types.DOUBLE.equals(type)) {
                                            pageBuilder.setDouble(column, val.asDouble());
                                        }
                                        else if (Types.LONG.equals(type)) {
                                            pageBuilder.setLong(column, val.asLong());
                                        }
                                        else if (Types.BOOLEAN.equals(type)) {
                                            pageBuilder.setBoolean(column, val.asBoolean());
                                        }
                                        else if (Types.TIMESTAMP.equals(type)) {
                                            pageBuilder.setTimestamp(column, Timestamp.ofEpochSecond(val.asLong()));
                                        }
                                        else if (Types.JSON.equals(type)) {
                                            pageBuilder.setJson(column, new JsonParser().parse(val.asText()));
                                        }
                                    }
                                    pageBuilder.addRecord();
                                }
                            }
                            else {
                                logger.error("Internal API Error");
                                String responseBody = EntityUtils.toString(httpResponse.getEntity());
                                logger.error(responseBody);
                                throw new ExecutionInterruptedException(new Exception("Internal API Error"));
                            }
                        }
                    }
                }
                catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    throw new ExecutionInterruptedException(e);
                }
            }
        };
    }
}
