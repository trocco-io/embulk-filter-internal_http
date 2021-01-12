package org.embulk.filter.internal_http;

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.internal_http.InternalHttpFilterPlugin.PluginTask;
import org.embulk.spi.*;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.msgpack.value.Value;

import java.util.List;

import static org.embulk.filter.internal_http.InternalHttpFilterPlugin.Control;
import static org.embulk.spi.type.Types.*;
import static org.junit.Assert.*;
import static org.msgpack.value.ValueFactory.*;

public class TestInternalHttpFilterPlugin {
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this, 18888);

    private InternalHttpFilterPlugin internalHttpFilterPlugin;

    @Before
    public void createResources() {
        internalHttpFilterPlugin = new InternalHttpFilterPlugin();
    }

    private ConfigSource configFromYamlString(String... lines) {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yamlString);
    }

    private PluginTask taskFromYamlString(String... lines) {
        ConfigSource config = configFromYamlString(lines);
        return config.loadConfig(PluginTask.class);
    }

    private Schema schema(Column... columns) {
        return new Schema(Lists.newArrayList(columns));
    }

    // Config tests
    @Test
    public void testDefaultConfigValues() {
        PluginTask task = taskFromYamlString(
                "type: internal_http",
                "url: http://foo.bar.com:18888",
                "columns:",
                "  - {name: col1, type: timestamp, format: \"%Y-%m-%d\"}",
                "  - {name: col2, type: timestamp}"
        );
        assertFalse(task.getSampleDataMode());
        assertEquals(task.getColumns().get(1).getFormat(), "%Y-%m-%d %H:%M:%S %z");
    }

    @Test
    public void testThrowExceptionAbsentColumnName() {
        assertThrows(ConfigException.class, () -> taskFromYamlString(
                "type: internal_http",
                "url: http://foo.bar.com:18888",
                "columns:",
                "  - {name: col1, type: long}",
                "  - {type: timestamp}"
        ));
    }

    @Test
    public void testThrowExceptionAbsentColumnType() {
        assertThrows(ConfigException.class, () -> taskFromYamlString(
                "type: internal_http",
                "url: http://foo.bar.com:18888",
                "columns:",
                "  - {name: col1, type: long}",
                "  - {name: col2}"
        ));
    }

    @Test
    public void testThrowExceptionAbsentUrlWithNonSampleDataMode() {
        ConfigSource config = configFromYamlString(
                "type: internal_http",
                "columns:",
                "  - {name: col1, type: timestamp}"
        );
        final Schema inputSchema = schema();
        assertThrows(ConfigException.class, () -> {
            internalHttpFilterPlugin.transaction(config, inputSchema, new Control() {
                @Override
                public void run(TaskSource taskSource, Schema outputSchema) {

                }
            });
        });
    }

    @Test
    public void testThrowExceptionAbsentColumnsWithNonSampleDataMode() {
        ConfigSource config = configFromYamlString(
                "type: internal_http",
                "url: http://foo.bar.com:18888"
        );
        final Schema inputSchema = schema();
        assertThrows(ConfigException.class, () -> {
            internalHttpFilterPlugin.transaction(config, inputSchema, new Control() {
                @Override
                public void run(TaskSource taskSource, Schema outputSchema) {

                }
            });
        });
    }

    // Filter tests
    @Test
    public void testSampleDataModeOutput() {
        ConfigSource config = configFromYamlString(
                "type: internal_http",
                "sample_data_mode: true"
        );
        final Schema inputSchema = schema(
                new Column(0, "long_col", LONG),
                new Column(1, "double_col", DOUBLE),
                new Column(2, "boolean_col", BOOLEAN),
                new Column(3, "string_col", STRING),
                new Column(4, "timestamp_col", TIMESTAMP),
                new Column(5, "json_col", JSON)
        );
        internalHttpFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = internalHttpFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            1L,
                            2.0,
                            false,
                            "foo",
                            Timestamp.ofEpochSecond(4),
                            newMapBuilder().put(newString("foo"), newString("bar")).build()
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                assertEquals(1, records.size());

                Object[] record = records.get(0);
                Value expectedData = newMapBuilder()
                        .put(newString("long_col"), newInteger(1))
                        .put(newString("double_col"), newFloat(2.0))
                        .put(newString("boolean_col"), newBoolean(false))
                        .put(newString("string_col"), newString("foo"))
                        .put(newString("timestamp_col"), newString("1970-01-01 00:00:04 +0000"))
                        .put(newString("json_col"), newMapBuilder().put(newString("foo"), newString("bar")).build())
                        .build();
                Value expectedSchema = newArray(
                        newArray(newString("long_col"), newString("long")),
                        newArray(newString("double_col"), newString("double")),
                        newArray(newString("boolean_col"), newString("boolean")),
                        newArray(newString("string_col"), newString("string")),
                        newArray(newString("timestamp_col"), newString("timestamp")),
                        newArray(newString("json_col"), newString("json"))
                );
                assertEquals(expectedData, record[0]);
                assertEquals(expectedSchema, record[1]);
            }
        });
    }

    @Test
    public void testNonSampleModeOutput() {
        new MockServerClient("localhost", 18888)
                .when(
                        HttpRequest.request()
                                .withMethod("POST")
                                .withPath("/")
                                .withBody("{\"rows\":[{\"long_col\":1,\"double_col\":2.0,\"boolean_col\":false,\"string_col\":\"foo\",\"timestamp_col\":\"1970-01-01 00:00:04 +0000\",\"json_col\":{\"foo\":\"bar\"}}]}")
                )
                .respond(
                        HttpResponse.response()
                                .withStatusCode(200)
                                .withBody("{\"rows\":[{\"long_col\":2,\"double_col\":3.0,\"boolean_col\":true,\"string_col\":\"bar\",\"timestamp_col\":\"1970-01-01 00:00:05 +0000\",\"json_col\":{\"bar\":\"baz\"}}]}")
                );
        ConfigSource config = configFromYamlString(
                "type: internal_http",
                "url: http://localhost:18888",
                "columns:",
                "  - {name: long_col, type: long}",
                "  - {name: double_col, type: double}",
                "  - {name: boolean_col, type: boolean}",
                "  - {name: string_col, type: string}",
                "  - {name: timestamp_col, type: timestamp}",
                "  - {name: json_col, type: json}"
        );
        final Schema inputSchema = schema(
                new Column(0, "long_col", LONG),
                new Column(1, "double_col", DOUBLE),
                new Column(2, "boolean_col", BOOLEAN),
                new Column(3, "string_col", STRING),
                new Column(4, "timestamp_col", TIMESTAMP),
                new Column(5, "json_col", JSON)
        );
        internalHttpFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = internalHttpFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            1L,
                            2.0,
                            false,
                            "foo",
                            Timestamp.ofEpochSecond(4),
                            newMapBuilder().put(newString("foo"), newString("bar")).build()
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                assertEquals(1, records.size());

                Object[] record = records.get(0);
                assertEquals(2L, record[0]);
                assertEquals(3.0, record[1]);
                assertEquals(true, record[2]);
                assertEquals("bar", record[3]);
                assertEquals(Timestamp.ofEpochSecond(5), record[4]);
                assertEquals(newMapBuilder().put(newString("bar"), newString("baz")).build(), record[5]);
            }
        });
    }

    @Test
    public void testNonSampleModeWithNullRowOutput() {
        new MockServerClient("localhost", 18888)
                .when(
                        HttpRequest.request()
                                .withMethod("POST")
                                .withPath("/")
                                .withBody("{\"rows\":[{\"long_col\":1}]}")
                )
                .respond(
                        HttpResponse.response()
                                .withStatusCode(200)
                                .withBody("{\"rows\":[null]}")
                );
        ConfigSource config = configFromYamlString(
                "type: internal_http",
                "url: http://localhost:18888",
                "columns:",
                "  - {name: long_col, type: long}"
        );
        final Schema inputSchema = schema(
                new Column(0, "long_col", LONG)
        );
        internalHttpFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = internalHttpFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            1L
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                assertEquals(0, records.size());
            }
        });
    }

    @Test
    public void testNonSampleModeWithNullFieldsOutput() {
        new MockServerClient("localhost", 18888)
                .when(
                        HttpRequest.request()
                                .withMethod("POST")
                                .withPath("/")
                                .withBody("{\"rows\":[{\"long_col\":1,\"double_col\":2.0,\"boolean_col\":false,\"string_col\":\"foo\",\"timestamp_col\":\"1970-01-01 00:00:04 +0000\",\"json_col\":{\"foo\":\"bar\"}}]}")
                )
                .respond(
                        HttpResponse.response()
                                .withStatusCode(200)
                                .withBody("{\"rows\":[{\"long_col\":null,\"double_col\":null,\"boolean_col\":null,\"string_col\":null,\"timestamp_col\":null,\"json_col\":null}]}")
                );
        ConfigSource config = configFromYamlString(
                "type: internal_http",
                "url: http://localhost:18888",
                "columns:",
                "  - {name: long_col, type: long}",
                "  - {name: double_col, type: double}",
                "  - {name: boolean_col, type: boolean}",
                "  - {name: string_col, type: string}",
                "  - {name: timestamp_col, type: timestamp}",
                "  - {name: json_col, type: json}"
        );
        final Schema inputSchema = schema(
                new Column(0, "long_col", LONG),
                new Column(1, "double_col", DOUBLE),
                new Column(2, "boolean_col", BOOLEAN),
                new Column(3, "string_col", STRING),
                new Column(4, "timestamp_col", TIMESTAMP),
                new Column(5, "json_col", JSON)
        );
        internalHttpFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = internalHttpFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            1L,
                            2.0,
                            false,
                            "foo",
                            Timestamp.ofEpochSecond(4),
                            newMapBuilder().put(newString("foo"), newString("bar")).build()
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                assertEquals(1, records.size());

                Object[] record = records.get(0);
                assertNull(record[0]);
                assertNull(record[1]);
                assertNull(record[2]);
                assertNull(record[3]);
                assertNull(record[4]);
                assertNull(record[5]);
            }
        });
    }

    @Test
    public void testNonSampleModeWithNonDefaultFormatTsOutput() {
        new MockServerClient("localhost", 18888)
                .when(
                        HttpRequest.request()
                                .withMethod("POST")
                                .withPath("/")
                                .withBody("{\"rows\":[{\"timestamp_col\":\"1970-01-01 00:00:04 +0000\"}]}")
                )
                .respond(
                        HttpResponse.response()
                                .withStatusCode(200)
                                .withBody("{\"rows\":[{\"timestamp_col\":\"1970/01/01 000004 +0000\"}]}")
                );
        ConfigSource config = configFromYamlString(
                "type: internal_http",
                "url: http://localhost:18888",
                "columns:",
                "  - {name: timestamp_col, type: timestamp, format: \"%Y/%m/%d %H%M%S %z\"}"
        );
        final Schema inputSchema = schema(
                new Column(0, "timestamp_col", TIMESTAMP)
        );
        internalHttpFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = internalHttpFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            Timestamp.ofEpochSecond(4)
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                assertEquals(1, records.size());

                Object[] record = records.get(0);
                assertEquals(Timestamp.ofEpochSecond(4), record[0]);
            }
        });
    }
}
