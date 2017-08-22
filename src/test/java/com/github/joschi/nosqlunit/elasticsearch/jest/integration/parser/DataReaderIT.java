package com.github.joschi.nosqlunit.elasticsearch.jest.integration.parser;

import com.github.joschi.nosqlunit.elasticsearch.jest.ElasticsearchConfiguration;
import com.github.joschi.nosqlunit.elasticsearch.jest.integration.BaseIT;
import com.github.joschi.nosqlunit.elasticsearch.jest.parser.DataReader;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.settings.GetSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DataReaderIT extends BaseIT {
    private static final String ELASTICSEARCH_DATA = "{\n" +
            "   \"documents\":[\n" +
            "      {\n" +
            "         \"document\":[\n" +
            "            {\n" +
            "               \"index\":{\n" +
            "                  \"indexName\":\"tweeter\",\n" +
            "                  \"indexType\":\"tweet\",\n" +
            "                  \"indexId\":\"1\"\n" +
            "               }\n" +
            "            },\n" +
            "            {\n" +
            "               \"data\":{\n" +
            "                  \"name\":\"a\",\n" +
            "                  \"msg\":\"b\"\n" +
            "               }\n" +
            "            }\n" +
            "         ]\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    private JestClient client;

    @Before
    public void setUp() {
        final ElasticsearchConfiguration configuration = ElasticsearchConfiguration.remoteElasticsearch(getServer()).build();
        client = configuration.getClient();
    }

    @After
    public void tearDown() throws IOException {
        final DeleteIndex deleteIndex = new DeleteIndex.Builder("tweeter").build();
        final JestResult result = client.execute(deleteIndex);

        assertThat(result.getErrorMessage(), result.isSucceeded(), is(true));

        if (client != null) {
            client.shutdownClient();
        }
    }

    @Test
    public void data_should_be_indexed() throws IOException {
        final DataReader dataReader = new DataReader(client, false, Collections.emptyMap(), Collections.emptyMap());
        dataReader.read(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        final Get getRequest = new Get.Builder("tweeter", "1").type("tweet").build();
        final DocumentResult response = client.execute(getRequest);
        assertThat(response.getErrorMessage(), response.isSucceeded(), is(true));

        @SuppressWarnings("unchecked") final Map<String, Object> document = response.getSourceAsObject(Map.class);

        assertThat(document.get("name"), is("a"));
        assertThat(document.get("msg"), is("b"));
    }

    @Test
    public void indices_should_be_created_with_custom_settings() throws IOException {
        final Map<String, Object> settings = Collections.singletonMap("settings", Collections.singletonMap("codec", "best_compression"));
        final DataReader dataReader = new DataReader(client, true, settings, Collections.emptyMap());
        dataReader.read(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        final GetSettings getSettings = new GetSettings.Builder().addIndex("tweeter").build();
        final JestResult response = client.execute(getSettings);

        assertThat(response.getErrorMessage(), response.isSucceeded(), is(true));

        final String codec = response.getJsonObject()
                .getAsJsonObject("tweeter")
                .getAsJsonObject("settings")
                .getAsJsonObject("index")
                .getAsJsonPrimitive("codec")
                .getAsString();
        assertThat(codec, is("best_compression"));
    }

    @Test
    public void indices_should_be_created_from_template() throws IOException {
        final Map<String, Object> templateSource = new HashMap<>();
        templateSource.put("template", "tweeter");
        templateSource.put("settings", Collections.singletonMap("codec", "best_compression"));
        final Map<String, Map<String, Object>> templates = Collections.singletonMap("data_reader_test", templateSource);
        final DataReader dataReader = new DataReader(client, true, Collections.emptyMap(), templates);
        dataReader.read(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        final GetSettings getSettings = new GetSettings.Builder().addIndex("tweeter").build();
        final JestResult indexSettingsResponse = client.execute(getSettings);

        assertThat(indexSettingsResponse.getErrorMessage(), indexSettingsResponse.isSucceeded(), is(true));

        final String codec = indexSettingsResponse.getJsonObject()
                .getAsJsonObject("tweeter")
                .getAsJsonObject("settings")
                .getAsJsonObject("index")
                .getAsJsonPrimitive("codec")
                .getAsString();
        assertThat(codec, is("best_compression"));
    }
}
