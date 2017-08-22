package com.github.joschi.nosqlunit.elasticsearch.jest.parser;

import com.google.gson.Gson;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.Refresh;
import io.searchbox.indices.template.DeleteTemplate;
import io.searchbox.indices.template.PutTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataReader {
    private static final Gson GSON = new Gson();

    public static final String DOCUMENTS_ELEMENT = "documents";
    public static final String DOCUMENT_ELEMENT = "document";
    public static final String DATA_ELEMENT = "data";
    public static final String INDEX_ELEMENT = "index";
    public static final String INDEX_NAME_ELEMENT = "indexName";
    public static final String INDEX_TYPE_ELEMENT = "indexType";
    public static final String INDEX_ID_ELEMENT = "indexId";

    private final JestClient client;
    private final boolean createIndices;
    private final Map<String, Object> indexSettings;
    private final Map<String, Map<String, Object>> templates;

    public DataReader(JestClient client,
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
            final DeleteTemplate deleteTemplate = new DeleteTemplate.Builder(template).build();
            final JestResult result = client.execute(deleteTemplate);
            if (!result.isSucceeded()) {
                throw new IllegalStateException("Error while deleting template \"" + template + "\": " + result.getErrorMessage());
            }
        }
    }

    private void createTemplates(Map<String, Map<String, Object>> templates) throws IOException {
        for (Map.Entry<String, Map<String, Object>> template : templates.entrySet()) {
            final String templateName = template.getKey();
            final PutTemplate putTemplate = new PutTemplate.Builder(templateName, template.getValue()).build();
            final JestResult result = client.execute(putTemplate);
            if (!result.isSucceeded()) {
                throw new IllegalStateException("Error while creating template \"" + templateName + "\": " + result.getErrorMessage());
            }
        }
    }

    private void refreshNode() {
        final Refresh request = new Refresh.Builder().build();
        try {
            final JestResult result = client.execute(request);
            if (!result.isSucceeded()) {
                throw new IllegalStateException("Error while refreshing indices: " + result.getErrorMessage());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to refresh indices", e);
        }
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

                final CreateIndex request = new CreateIndex.Builder(indexName)
                        .settings(indexSettings)
                        .build();
                final JestResult result = client.execute(request);
                if (!result.isSucceeded()) {
                    throw new IllegalStateException("Error while creating index " + indexName + ": " + result.getErrorMessage());
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
        final Bulk.Builder bulkBuilder = new Bulk.Builder();
        for (Map<String, String> indexInformation : indexes) {
            final Index indexDocument = indexDocument(indexInformation, dataOfDocument);
            bulkBuilder.addAction(indexDocument);
        }

        final BulkResult result = client.execute(bulkBuilder.build());
        if (!result.isSucceeded()) {
            throw new IllegalStateException("Error while bulk indexing documents: " + result.getErrorMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> dataOfDocument(Object object) {
        return (Map<String, Object>) object;
    }

    private Index indexDocument(Map<String, String> indexInformation, Object source) {
        if (!indexInformation.containsKey(INDEX_NAME_ELEMENT)) {
            throw new IllegalArgumentException("Missing index name element in " + indexInformation);
        }

        final Index.Builder createIndexBuilder = new Index.Builder(source)
                .index(indexInformation.get(INDEX_NAME_ELEMENT));

        if (indexInformation.containsKey(INDEX_TYPE_ELEMENT)) {
            createIndexBuilder.type(indexInformation.get(INDEX_TYPE_ELEMENT));
        }

        if (indexInformation.containsKey(INDEX_ID_ELEMENT)) {
            createIndexBuilder.id(indexInformation.get(INDEX_ID_ELEMENT));
        }

        return createIndexBuilder.build();
    }

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> getDocuments(InputStream data) throws IOException {
        try (Reader reader = new InputStreamReader(data)) {
            final Map<String, Object> rootNode = (Map<String, Object>) GSON.fromJson(reader, Map.class);
            final Object dataElements = rootNode.get(DOCUMENTS_ELEMENT);

            if (dataElements instanceof List) {
                return (List<Map<String, Object>>) dataElements;
            } else {
                throw new IllegalArgumentException("Array of documents are required.");
            }
        }
    }
}
