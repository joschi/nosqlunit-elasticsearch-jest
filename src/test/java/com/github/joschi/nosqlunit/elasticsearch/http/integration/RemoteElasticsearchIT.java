package com.github.joschi.nosqlunit.elasticsearch.http.integration;

import com.github.joschi.nosqlunit.elasticsearch.http.ElasticsearchConfiguration;
import com.github.joschi.nosqlunit.elasticsearch.http.ElasticsearchOperation;
import io.searchbox.client.JestClient;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.Refresh;
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

    private final JestClient client;
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
        final DeleteIndex deleteIndex = new DeleteIndex.Builder("tweeter").build();
        client.execute(deleteIndex);
        final Refresh refresh = new Refresh.Builder().build();
        client.execute(refresh);
    }

    @Test
    public void insert_operation_should_index_all_dataset() throws IOException {
        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        final Get getRequest = new Get.Builder("tweeter", "1").type("tweet").build();
        final DocumentResult documentResult = client.execute(getRequest);
        @SuppressWarnings("unchecked")
        Map<String, Object> documentSource = documentResult.getSourceAsObject(Map.class);

        //Strange a cast to Object
        assertThat(documentSource, hasEntry("name", "a"));
        assertThat(documentSource, hasEntry("msg", "b"));
    }

    @Test
    public void delete_operation_should_remove_all_Indexes() throws IOException {
        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        elasticsearchOperation.deleteAll();

        final Get getRequest = new Get.Builder("tweeter", "1").type("tweet").build();
        final DocumentResult documentResult = client.execute(getRequest);
        assertThat(documentResult.getSourceAsStringList().isEmpty(), is(false));
    }

    @Test
    public void databaseIs_operation_should_compare_all_Indexes() {
        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        boolean isEqual = elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        assertThat(isEqual, is(true));
    }
}
