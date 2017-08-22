package com.github.joschi.nosqlunit.elasticsearch.jest;

import com.github.joschi.nosqlunit.elasticsearch.jest.parser.DataReader;

import java.io.InputStream;
import java.util.Map;

public class DefaultElasticsearchInsertionStrategy implements ElasticsearchInsertionStrategy {
    private final boolean createIndices;
    private final Map<String, Object> indexSettings;
    private final Map<String, Map<String, Object>> templates;

    public DefaultElasticsearchInsertionStrategy(boolean createIndices,
                                                 Map<String, Object> indexSettings,
                                                 Map<String, Map<String, Object>> templates) {
        this.createIndices = createIndices;
        this.indexSettings = indexSettings;
        this.templates = templates;
    }

    @Override
    public void insert(ElasticsearchConnectionCallback connection, InputStream dataset) throws Throwable {
        DataReader dataReader = new DataReader(connection.client(), createIndices, indexSettings, templates);
        dataReader.read(dataset);
    }
}
