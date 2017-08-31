package com.github.joschi.nosqlunit.elasticsearch.jest;

import com.github.joschi.nosqlunit.elasticsearch.jest.parser.DataReader;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class DefaultElasticsearchComparisonStrategy implements ElasticsearchComparisonStrategy {
    @Override
    public boolean compare(ElasticsearchConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError,
            Throwable {
        final RestHighLevelClient jestClient = connection.client();
        final List<Map<String, Object>> documents = DataReader.getDocuments(dataset);
        ElasticsearchAssertion.strictAssertEquals(documents, jestClient);
        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
        // NOP
    }
}
