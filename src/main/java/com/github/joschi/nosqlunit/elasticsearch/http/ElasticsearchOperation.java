package com.github.joschi.nosqlunit.elasticsearch.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Count;
import io.searchbox.core.CountResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.Refresh;
import io.searchbox.params.Parameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

public class ElasticsearchOperation extends
        AbstractCustomizableDatabaseOperation<ElasticsearchConnectionCallback, JestClient> {

    private final JestClient client;
    private final boolean deleteAllIndices;

    public ElasticsearchOperation(JestClient client,
                                  boolean deleteAllIndices,
                                  boolean createIndices,
                                  Map<String, Object> indexSettings,
                                  Map<String, Map<String, Object>> templates) {
        this.client = client;
        this.deleteAllIndices = deleteAllIndices;

        setInsertionStrategy(new DefaultElasticsearchInsertionStrategy(createIndices, indexSettings, templates));
        setComparisonStrategy(new DefaultElasticsearchComparisonStrategy());
    }

    @Override
    public void insert(InputStream dataScript) {
        insertData(dataScript);
    }

    private void insertData(InputStream dataScript) {
        try {
            executeInsertion(() -> client, dataScript);
        } catch (Throwable e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteAll() {
        try {
            clearDocuments();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void clearDocuments() throws IOException {
        final long documentCount = documentCount();

        if (deleteAllIndices) {
            final DeleteIndex deleteIndex = new DeleteIndex.Builder("*").build();
            final JestResult result = client.execute(deleteIndex);
            if (!result.isSucceeded()) {
                throw new IllegalStateException(result.getErrorMessage());
            }

            refreshNode();
        } else if (documentCount > 0) {
            final Search initialScroll = new Search.Builder("{\"query\":{\"match_all\" : {}}}")
                    .setParameter(Parameters.SCROLL, "1m")
                    .setParameter(Parameters.SIZE, documentCount)
                    .build();
            final SearchResult scrollResponse = client.execute(initialScroll);
            if (!scrollResponse.isSucceeded()) {
                throw new IllegalStateException(scrollResponse.getErrorMessage());
            }

            final Bulk.Builder bulkRequestBuilder = new Bulk.Builder();
            int numberOfActions = 0;
            while (true) {
                final String scrollId = scrollResponse.getJsonObject().path("_scroll_id").asText();
                final SearchScroll searchScroll = new SearchScroll.Builder(scrollId, "1m").build();
                final JestResult result = client.execute(searchScroll);
                if (!result.isSucceeded()) {
                    throw new IllegalStateException(result.getErrorMessage());
                }
                final JsonNode hits = result.getJsonObject().path("hits").path("hits");

                // Break condition: No hits are returned
                if (hits.size() == 0) {
                    break;
                }

                for (JsonNode hit : hits) {
                    if (hit.isObject()) {
                        final Delete deleteAction = new Delete.Builder(hit.path("id").asText())
                                .index(hit.path("index").asText())
                                .type(hit.path("type").asText())
                                .build();
                        bulkRequestBuilder.addAction(deleteAction);
                        numberOfActions++;
                    }
                }
            }

            if (numberOfActions > 0) {
                final BulkResult bulkResponse = client.execute(bulkRequestBuilder.build());
                if (!bulkResponse.isSucceeded()) {
                    throw new IllegalStateException(bulkResponse.getErrorMessage());
                }
            }

            refreshNode();
        }

    }

    private long documentCount() throws IOException {
        final CountResult countResult = client.execute(new Count.Builder().build());
        if (!countResult.isSucceeded()) {
            throw new IllegalStateException(countResult.getErrorMessage());
        }
        return countResult.getCount().longValue();
    }

    private void refreshNode() throws IOException {
        final JestResult result = client.execute(new Refresh.Builder().build());
        if (!result.isSucceeded()) {
            throw new IllegalStateException(result.getErrorMessage());
        }
    }

    @Override
    public boolean databaseIs(InputStream expectedData) {
        try {
            return executeComparison(() -> client, expectedData);
        } catch (NoSqlAssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public JestClient connectionManager() {
        return client;
    }
}
