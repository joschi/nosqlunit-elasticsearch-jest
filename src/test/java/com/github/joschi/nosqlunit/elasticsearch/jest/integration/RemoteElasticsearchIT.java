package com.github.joschi.nosqlunit.elasticsearch.jest.integration;

import com.github.joschi.nosqlunit.elasticsearch.jest.ElasticsearchConfiguration;
import com.github.joschi.nosqlunit.elasticsearch.jest.ElasticsearchOperation;
import com.github.joschi.nosqlunit.elasticsearch.jest.RestClientHelper;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;

public class RemoteElasticsearchIT extends BaseIT {
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

    private final RestHighLevelClient client;
    private ElasticsearchOperation elasticsearchOperation;

    public RemoteElasticsearchIT() throws IOException {

        final ElasticsearchConfiguration configuration = ElasticsearchConfiguration
                .remoteElasticsearch(getServer())
                .build();
        this.client = configuration.getClient();
    }

    @Before
    public void setUp() {
        elasticsearchOperation = new ElasticsearchOperation(
                client,
                false,
                false,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    @After
    public void removeIndexes() throws IOException {
        RestClientHelper.deleteIndex(client.getLowLevelClient(), Collections.singleton("tweeter"));
        RestClientHelper.refresh(client.getLowLevelClient(), Collections.emptySet());
    }

    @Test
    public void insert_operation_should_index_all_dataset() throws IOException {
        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        final GetRequest getRequest = new GetRequest("tweeter", "tweet", "1");
        final GetResponse getResponse = client.get(getRequest);

        Map<String, Object> documentSource = getResponse.getSourceAsMap();

        //Strange a cast to Object
        assertThat(documentSource, hasEntry("name", "a"));
        assertThat(documentSource, hasEntry("msg", "b"));
    }

    @Test
    public void delete_operation_should_remove_all_Indexes() throws IOException {
        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        elasticsearchOperation.deleteAll();

        final GetRequest getRequest = new GetRequest("tweeter", "tweet", "1");
        final GetResponse getResponse = client.get(getRequest);
        assertThat(getResponse.isExists(), is(false));
    }

    @Test
    public void databaseIs_operation_should_compare_all_Indexes() {
        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        boolean isEqual = elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        assertThat(isEqual, is(true));
    }
}
