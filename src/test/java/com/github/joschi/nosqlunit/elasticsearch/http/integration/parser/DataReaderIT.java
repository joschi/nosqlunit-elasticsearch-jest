package com.github.joschi.nosqlunit.elasticsearch.http.integration.parser;

import com.github.joschi.nosqlunit.elasticsearch.http.ElasticsearchConfiguration;
import com.github.joschi.nosqlunit.elasticsearch.http.RestClientHelper;
import com.github.joschi.nosqlunit.elasticsearch.http.integration.BaseIT;
import com.github.joschi.nosqlunit.elasticsearch.http.parser.DataReader;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestHighLevelClient;
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

    private RestHighLevelClient client;

    @Before
    public void setUp() {
        final ElasticsearchConfiguration configuration = ElasticsearchConfiguration.remoteElasticsearch(getServer()).build();
        client = configuration.getClient();
    }

    @After
    public void tearDown() throws IOException {
        RestClientHelper.deleteIndex(client.getLowLevelClient(), Collections.emptySet());

        if (client != null) {
            client.close();
        }
    }

    @Test
    public void data_should_be_indexed() throws IOException {
        final DataReader dataReader = new DataReader(client, false, Collections.emptyMap(), Collections.emptyMap());
        dataReader.read(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        final GetRequest getRequest = new GetRequest("tweeter", "tweet", "1");
        final GetResponse response = client.get(getRequest);
        assertThat(response.isExists(), is(true));

        final Map<String, Object> document = response.getSourceAsMap();

        assertThat(document.get("name"), is("a"));
        assertThat(document.get("msg"), is("b"));
    }

    @Test
    public void indices_should_be_created_with_custom_settings() throws IOException {
        final Map<String, Object> settings = Collections.singletonMap("settings", Collections.singletonMap("codec", "best_compression"));
        final DataReader dataReader = new DataReader(client, true, settings, Collections.emptyMap());
        dataReader.read(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        final Map<String, Object> settingsResponse = RestClientHelper.getSettings(client.getLowLevelClient(), Collections.singleton("tweeter"));

        final Map tweeterMap = (Map) settingsResponse.get("tweeter");
        final Map settingsMap = (Map) tweeterMap.get("settings");
        final Map indexMap = (Map) settingsMap.get("index");
        final String codec = (String) indexMap.get("codec");

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

        final Map<String, Object> settingsResponse = RestClientHelper.getSettings(client.getLowLevelClient(), Collections.singleton("tweeter"));

        final Map tweeterMap = (Map) settingsResponse.get("tweeter");
        final Map settingsMap = (Map) tweeterMap.get("settings");
        final Map indexMap = (Map) settingsMap.get("index");
        final String codec = (String) indexMap.get("codec");

        assertThat(codec, is("best_compression"));
    }
}
