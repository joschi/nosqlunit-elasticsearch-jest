package com.github.joschi.nosqlunit.elasticsearch.jest.integration;

import com.github.joschi.nosqlunit.elasticsearch.jest.ElasticsearchConfiguration;
import com.github.joschi.nosqlunit.elasticsearch.jest.ElasticsearchOperation;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class WhenExpectedDataShouldBeComparedIT extends BaseIT {

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

    private static final String ELASTICSEARCH_TWO_DATA = "{\n" +
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
            "      },\n" +
            "      {\n" +
            "         \"document\":[\n" +
            "            {\n" +
            "               \"index\":{\n" +
            "                  \"indexName\":\"tweeter\",\n" +
            "                  \"indexType\":\"tweet\",\n" +
            "                  \"indexId\":\"2\"\n" +
            "               }\n" +
            "            },\n" +
            "            {\n" +
            "               \"data\":{\n" +
            "                  \"name\":\"c\",\n" +
            "                  \"msg\":\"d\"\n" +
            "               }\n" +
            "            }\n" +
            "         ]\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    private static final String ELASTICSEARCH_DATA_INDEX_NOT_FOUND = "{\n" +
            "   \"documents\":[\n" +
            "      {\n" +
            "         \"document\":[\n" +
            "            {\n" +
            "               \"index\":{\n" +
            "                  \"indexName\":\"tweeter\",\n" +
            "                  \"indexType\":\"tweet\",\n" +
            "                  \"indexId\":\"2\"\n" +
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

    private static final String ELASTICSEARCH_DATA_NOT_FOUND = "{\n" +
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
            "                  \"msg\":\"c\"\n" +
            "               }\n" +
            "            }\n" +
            "         ]\n" +
            "      }\n" +
            "   ]\n" +
            "}";


    private final DatabaseOperation elasticsearchOperation;

    public WhenExpectedDataShouldBeComparedIT() throws IOException {
        final ElasticsearchConfiguration configuration = ElasticsearchConfiguration
                .remoteElasticsearch(getServer())
                .build();
        this.elasticsearchOperation = new ElasticsearchOperation(
                configuration.getClient(),
                configuration.isDeleteAllIndices(),
                configuration.isCreateIndices(),
                configuration.getIndexSettings(),
                configuration.getIndexTemplates());
    }

    @After
    public void tearDown() {
        elasticsearchOperation.deleteAll();
    }

    @Test
    public void no_exception_should_be_thrown_if_data_is_expected() {

        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        boolean result = elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        assertThat(result, is(true));

    }

    @Test
    public void exception_should_be_thrown_if_different_number_of_documents() {

        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        try {
            elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_TWO_DATA.getBytes()));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected number of documents are 2 but 1 has been found."));
        }
    }

    @Test
    public void exception_should_be_thrown_if_index_not_found() {

        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        try {
            elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_DATA_INDEX_NOT_FOUND.getBytes()));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Document with index: tweeter - type: tweet - id: 2 has not returned any document."));
        }
    }

    @Test
    public void exception_should_be_thrown_if_different_() {
        elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

        try {
            elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_DATA_NOT_FOUND.getBytes()));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), startsWith("Expected document for index: tweeter - type: tweet - id: 1 is {\"msg\":\"c\",\"name\":\"a\"}, but {\"msg\":\"b\",\"name\":\"a\"} was found."));
        }
    }

}
