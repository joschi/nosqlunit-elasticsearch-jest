package com.github.joschi.nosqlunit.elasticsearch.http;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Count;
import io.searchbox.core.CountResult;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.Refresh;
import io.searchbox.params.Parameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
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
            final Collection<BulkableAction<DocumentResult>> bulkActions = new ArrayList<>();

            final Search initialScroll = new Search.Builder("{\"query\":{\"match_all\" : {}}}")
                    .setParameter(Parameters.SCROLL, "1m")
                    .setParameter(Parameters.SIZE, documentCount)
                    .build();
            final SearchResult scrollResponse = client.execute(initialScroll);
            if (!scrollResponse.isSucceeded()) {
                throw new IllegalStateException(scrollResponse.getErrorMessage());
            }
            bulkActions.addAll(prepareDelete(scrollResponse));

            final String scrollId = scrollResponse.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();
            while (true) {
                final SearchScroll searchScroll = new SearchScroll.Builder(scrollId, "1m").build();
                final JestResult result = client.execute(searchScroll);
                if (!result.isSucceeded()) {
                    throw new IllegalStateException(result.getErrorMessage());
                }

                final Collection<BulkableAction<DocumentResult>> deleteRequests = prepareDelete(result);

                if (deleteRequests.isEmpty()) {
                    break;
                } else {
                    bulkActions.addAll(deleteRequests);
                }
            }

            if (!bulkActions.isEmpty()) {
                final Bulk.Builder bulkRequestBuilder = new Bulk.Builder().addAction(bulkActions);
                final BulkResult bulkResponse = client.execute(bulkRequestBuilder.build());
                if (!bulkResponse.isSucceeded()) {
                    throw new IllegalStateException(bulkResponse.getErrorMessage());
                }
            }

            refreshNode();
        }

    }

    private Collection<BulkableAction<DocumentResult>> prepareDelete(JestResult result) {
        final JsonArray hits = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
        final Collection<BulkableAction<DocumentResult>> bulkActions = new ArrayList<>(hits.size());

        for (JsonElement element : hits) {
            if (element.isJsonObject()) {
                final JsonObject hit = element.getAsJsonObject();
                final Delete deleteAction = new Delete.Builder(hit.getAsJsonPrimitive("_id").getAsString())
                        .index(hit.getAsJsonPrimitive("_index").getAsString())
                        .type(hit.getAsJsonPrimitive("_type").getAsString())
                        .build();
                bulkActions.add(deleteAction);
            }
        }

        return bulkActions;
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
