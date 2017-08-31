package com.github.joschi.nosqlunit.elasticsearch.jest;

import com.github.joschi.nosqlunit.elasticsearch.jest.parser.DataReader;
import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.util.DeepEquals;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchAssertion {
    private ElasticsearchAssertion() {
        super();
    }

    @SuppressWarnings("unchecked")
    public static void strictAssertEquals(List<Map<String, Object>> expectedDocuments, RestHighLevelClient client) throws IOException {

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
                                                 RestHighLevelClient client) throws IOException {
        for (Map<String, Object> indexInformation : indexes) {
            final GetRequest request = prepareGet(indexInformation);
            final GetResponse documentResult = client.get(request);
            checkExistenceOfDocument(documentResult);
            checkDocumentEquality(expectedDataOfDocument, documentResult);
        }
    }

    private static void checkDocumentEquality(Map<String, Object> expectedDataOfDocument,
                                              GetResponse response) throws IOException {
        final Map<String, Object> dataOfDocument = response.getSourceAsMap();
        final XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();

        // Workaround because DeepEquals.deepEquals expects the types to be identical
        final Map<String, Object> actual = new HashMap<>(dataOfDocument);
        final Map<String, Object> expected = new HashMap<>(expectedDataOfDocument);
        if (!DeepEquals.deepEquals(actual, expected)) {
            throw FailureHandler.createFailure("Expected document for index: %s - type: %s - id: %s is %s, but %s was found.",
                    response.getIndex(), response.getType(), response.getId(),
                    jsonBuilder.map(expectedDataOfDocument).string(), response.getSourceAsString());
        }
    }

    private static void checkExistenceOfDocument(GetResponse response) {
        if (!response.isExists()) {
            throw FailureHandler.createFailure(
                    "Document with index: %s - type: %s - id: %s has not returned any document.",
                    response.getIndex(), response.getType(), response.getId());
        }
    }

    private static void checkNumberOfDocuments(List<Map<String, Object>> expectedDocuments, RestHighLevelClient client) throws IOException {
        int expectedNumberOfElements = expectedDocuments.size();

        long numberOfInsertedDocuments = numberOfInsertedDocuments(client);

        if (expectedNumberOfElements != numberOfInsertedDocuments) {
            throw FailureHandler.createFailure("Expected number of documents are %s but %s has been found.",
                    expectedNumberOfElements, numberOfInsertedDocuments);
        }
    }

    private static GetRequest prepareGet(Map<String, Object> indexInformation) {
        final String index = (String) indexInformation.get(DataReader.INDEX_NAME_ELEMENT);
        final String id = (String) indexInformation.get(DataReader.INDEX_ID_ELEMENT);
        final GetRequest getRequest = new GetRequest(index).id(id);

        if (indexInformation.containsKey(DataReader.INDEX_TYPE_ELEMENT)) {
            getRequest.type((String) indexInformation.get(DataReader.INDEX_TYPE_ELEMENT));
        }

        return getRequest;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> dataOfDocument(Object object) {
        return (Map<String, Object>) object;
    }

    private static long numberOfInsertedDocuments(RestHighLevelClient client) throws IOException {
        return RestClientHelper.count(client, Collections.emptySet(), Collections.emptySet());
    }
}
