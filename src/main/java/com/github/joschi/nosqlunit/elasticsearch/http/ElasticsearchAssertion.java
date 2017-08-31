package com.github.joschi.nosqlunit.elasticsearch.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joschi.nosqlunit.elasticsearch.http.parser.DataReader;
import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.util.DeepEquals;
import io.searchbox.client.JestClient;
import io.searchbox.core.Count;
import io.searchbox.core.CountResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchAssertion {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ElasticsearchAssertion() {
        super();
    }

    @SuppressWarnings("unchecked")
    public static void strictAssertEquals(List<Map<String, Object>> expectedDocuments, JestClient client) throws IOException {

        checkNumberOfDocuments(expectedDocuments, client);

        for (Map<String, Object> document : expectedDocuments) {
            final Object object = document.get(DataReader.DOCUMENT_ELEMENT);

            if (object instanceof List) {
                final List<Map<String, Object>> properties = (List<Map<String, Object>>) object;

                final List<Map<String, Object>> indexes = new ArrayList<>();
                Map<String, Object> expectedDataOfDocument = new HashMap<>();

                for (Map<String, Object> property : properties) {

                    if (property.containsKey(DataReader.INDEX_ELEMENT)) {
                        indexes.add((Map<String, Object>) property.get(DataReader.INDEX_ELEMENT));
                    } else {
                        if (property.containsKey(DataReader.DATA_ELEMENT)) {
                            expectedDataOfDocument = dataOfDocument(property.get(DataReader.DATA_ELEMENT));
                        }
                    }

                }

                checkIndicesWithDocument(indexes, expectedDataOfDocument, client);

            } else {
                throw new IllegalArgumentException("Array of Indexes and Data are required.");
            }
        }
    }

    private static void checkIndicesWithDocument(List<Map<String, Object>> indexes,
                                                 Map<String, Object> expectedDataOfDocument,
                                                 JestClient client) throws IOException {
        for (Map<String, Object> indexInformation : indexes) {
            final Get request = prepareGet(indexInformation);
            final DocumentResult documentResult = client.execute(request);
            checkExistenceOfDocument(request, documentResult);
            checkDocumentEquality(expectedDataOfDocument, request, documentResult);
        }
    }

    private static void checkDocumentEquality(Map<String, Object> expectedDataOfDocument,
                                              Get request,
                                              DocumentResult dataOfDocumentResponse) throws JsonProcessingException {
        @SuppressWarnings("unchecked") final Map<String, Object> dataOfDocument = (Map<String, Object>) dataOfDocumentResponse.getSourceAsObject(Map.class, false);

        // Workaround because DeepEquals.deepEquals expects the types to be identical
        final Map<String, Object> actual = new HashMap<>(dataOfDocument);
        final Map<String, Object> expected = new HashMap<>(expectedDataOfDocument);
        if (!DeepEquals.deepEquals(actual, expected)) {
            throw FailureHandler.createFailure("Expected document for index: %s - type: %s - id: %s is %s, but %s was found.",
                    request.getIndex(), request.getType(), request.getId(),
                    OBJECT_MAPPER.writeValueAsString(expectedDataOfDocument), OBJECT_MAPPER.writeValueAsString(dataOfDocument));
        }
    }

    private static void checkExistenceOfDocument(Get request, DocumentResult dataOfDocumentResponse) {
        if (!dataOfDocumentResponse.isSucceeded()) {
            throw FailureHandler.createFailure(
                    "Document with index: %s - type: %s - id: %s has not returned any document.",
                    request.getIndex(), request.getType(), request.getId());
        }
    }

    private static void checkNumberOfDocuments(List<Map<String, Object>> expectedDocuments, JestClient client) throws IOException {
        int expectedNumberOfElements = expectedDocuments.size();

        long numberOfInsertedDocuments = numberOfInsertedDocuments(client);

        if (expectedNumberOfElements != numberOfInsertedDocuments) {
            throw FailureHandler.createFailure("Expected number of documents are %s but %s has been found.",
                    expectedNumberOfElements, numberOfInsertedDocuments);
        }
    }

    private static Get prepareGet(Map<String, Object> indexInformation) {
        final String index = (String) indexInformation.get(DataReader.INDEX_NAME_ELEMENT);
        final String id = (String) indexInformation.get(DataReader.INDEX_ID_ELEMENT);
        final Get.Builder getBuilder = new Get.Builder(index, id);

        if (indexInformation.containsKey(DataReader.INDEX_TYPE_ELEMENT)) {
            getBuilder.type((String) indexInformation.get(DataReader.INDEX_TYPE_ELEMENT));
        }

        return getBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> dataOfDocument(Object object) {
        return (Map<String, Object>) object;
    }

    private static long numberOfInsertedDocuments(JestClient client) throws IOException {
        final CountResult countResult = client.execute(new Count.Builder().build());
        return countResult.getCount().longValue();
    }
}
