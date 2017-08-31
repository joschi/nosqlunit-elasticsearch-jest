package com.github.joschi.nosqlunit.elasticsearch.jest.parser;

import com.github.joschi.nosqlunit.elasticsearch.jest.RestClientHelper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataReader {
    public static final String DOCUMENTS_ELEMENT = "documents";
    public static final String DOCUMENT_ELEMENT = "document";
    public static final String DATA_ELEMENT = "data";
    public static final String INDEX_ELEMENT = "index";
    public static final String INDEX_NAME_ELEMENT = "indexName";
    public static final String INDEX_TYPE_ELEMENT = "indexType";
    public static final String INDEX_ID_ELEMENT = "indexId";

    private final RestHighLevelClient client;
    private final boolean createIndices;
    private final Map<String, Object> indexSettings;
    private final Map<String, Map<String, Object>> templates;

    public DataReader(RestHighLevelClient client,
                      boolean createIndices,
                      Map<String, Object> indexSettings,
                      Map<String, Map<String, Object>> templates) {
        this.client = client;
        this.createIndices = createIndices;
        this.indexSettings = indexSettings;
        this.templates = templates;
    }

    public void read(InputStream data) {
        try {
            final List<Map<String, Object>> documents = getDocuments(data);
            if (!templates.isEmpty()) {
                createTemplates(templates);
            }

            if (createIndices) {
                createIndices(documents, indexSettings);
            }

            insertDocuments(documents);

            if (!templates.isEmpty()) {
                deleteTemplates(templates.keySet());
                refreshNode();
            }

            refreshNode();
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void deleteTemplates(Set<String> templates) throws IOException {
        for (String template : templates) {
            final boolean isSucceeded = RestClientHelper.deleteTemplate(client.getLowLevelClient(), template);
            if (!isSucceeded) {
                throw new IllegalStateException("Error while deleting template \"" + template);
            }
        }
    }

    private void createTemplates(Map<String, Map<String, Object>> templates) throws IOException {
        for (Map.Entry<String, Map<String, Object>> template : templates.entrySet()) {
            final String templateName = template.getKey();
            final boolean isSucceeded = RestClientHelper.createTemplate(client.getLowLevelClient(), templateName, template.getValue());
            if (!isSucceeded) {
                throw new IllegalStateException("Error while creating template \"" + templateName + "\"");
            }
        }
    }

    private void refreshNode() throws IOException {
        RestClientHelper.refresh(client.getLowLevelClient(), Collections.emptySet());
    }

    @SuppressWarnings("unchecked")
    private void createIndices(List<Map<String, Object>> documents, Map<String, Object> indexSettings) throws IOException {
        for (Map<String, Object> document : documents) {
            final Object object = document.get(DOCUMENT_ELEMENT);

            if (object instanceof List) {
                final List<Map<String, Object>> properties = (List<Map<String, Object>>) object;
                createIndex(properties, indexSettings);
            } else {
                throw new IllegalArgumentException("Array of Indexes and Data are required.");
            }
        }
    }

    private void insertDocuments(List<Map<String, Object>> documents) throws IOException {
        for (Map<String, Object> document : documents) {
            final Object object = document.get(DOCUMENT_ELEMENT);

            if (object instanceof List) {
                @SuppressWarnings("unchecked") final List<Map<String, Object>> properties = (List<Map<String, Object>>) object;
                insertDocument(properties);
            } else {
                throw new IllegalArgumentException("Array of Indexes and Data are required.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void createIndex(List<Map<String, Object>> properties, Map<String, Object> indexSettings) throws IOException {
        for (Map<String, Object> property : properties) {
            if (property.containsKey(INDEX_ELEMENT)) {
                final Map<String, String> indexInformation = (Map<String, String>) property.get(INDEX_ELEMENT);
                final String indexName = indexInformation.get(INDEX_NAME_ELEMENT);
                if (indexName == null) {
                    throw new IllegalArgumentException("Missing index name element in " + indexInformation);
                }

                final boolean isSucceeded = RestClientHelper.createIndex(client.getLowLevelClient(), indexName, indexSettings);
                if (!isSucceeded) {
                    throw new IllegalStateException("Error while creating index " + indexName);
                }
            }
        }
    }

    private void insertDocument(List<Map<String, Object>> properties) throws IOException {
        final List<Map<String, String>> indexes = new ArrayList<>();
        Map<String, Object> dataOfDocument = new HashMap<>();

        for (Map<String, Object> property : properties) {
            if (property.containsKey(INDEX_ELEMENT)) {
                @SuppressWarnings("unchecked") final Map<String, String> indexInformation = (Map<String, String>) property.get(INDEX_ELEMENT);
                indexes.add(indexInformation);
            } else {
                if (property.containsKey(DATA_ELEMENT)) {
                    dataOfDocument = dataOfDocument(property.get(DATA_ELEMENT));
                }
            }
        }

        insertIndexes(indexes, dataOfDocument);
    }

    private void insertIndexes(List<Map<String, String>> indexes, Map<String, Object> dataOfDocument) throws IOException {
        final BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> indexInformation : indexes) {
            final IndexRequest indexDocument = indexDocument(indexInformation, dataOfDocument);
            bulkRequest.add(indexDocument);
        }

        final BulkResponse bulkResponse = client.bulk(bulkRequest);
        if (bulkResponse.hasFailures()) {
            throw new IllegalStateException("Error while bulk indexing documents: " + bulkResponse.buildFailureMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> dataOfDocument(Object object) {
        return (Map<String, Object>) object;
    }

    private IndexRequest indexDocument(Map<String, String> indexInformation, Map<String, Object> source) {
        if (!indexInformation.containsKey(INDEX_NAME_ELEMENT)) {
            throw new IllegalArgumentException("Missing index name element in " + indexInformation);
        }

        final IndexRequest indexRequest = new IndexRequest(indexInformation.get(INDEX_NAME_ELEMENT))
                .source(source);

        if (indexInformation.containsKey(INDEX_TYPE_ELEMENT)) {
            indexRequest.type(indexInformation.get(INDEX_TYPE_ELEMENT));
        }

        if (indexInformation.containsKey(INDEX_ID_ELEMENT)) {
            indexRequest.id(indexInformation.get(INDEX_ID_ELEMENT));
        }

        return indexRequest;
    }

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> getDocuments(InputStream data) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, data)) {
            final Map<String, Object> rootNode = parser.map();
            final Object dataElements = rootNode.get(DOCUMENTS_ELEMENT);

            if (dataElements instanceof List) {
                return (List<Map<String, Object>>) dataElements;
            } else {
                throw new IllegalArgumentException("Array of documents are required.");
            }
        }
    }
}
