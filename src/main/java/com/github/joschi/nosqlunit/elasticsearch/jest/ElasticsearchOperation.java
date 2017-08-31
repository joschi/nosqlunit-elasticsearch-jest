package com.github.joschi.nosqlunit.elasticsearch.jest;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ElasticsearchOperation extends
        AbstractCustomizableDatabaseOperation<ElasticsearchConnectionCallback, RestHighLevelClient> {

    private final RestHighLevelClient client;
    private final boolean deleteAllIndices;

    public ElasticsearchOperation(RestHighLevelClient client,
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
            RestClientHelper.deleteIndex(client.getLowLevelClient(), Collections.emptySet());

            refreshNode();
        } else if (documentCount > 0) {
            final SearchSourceBuilder searchSource = new SearchSourceBuilder()
                    .query(QueryBuilders.matchAllQuery())
                    .fetchSource(false);

            final SearchRequest initialScroll = new SearchRequest()
                    .source(searchSource)
                    .scroll(TimeValue.timeValueMinutes(1L));

            final BulkRequest bulkRequest = new BulkRequest();
            int numberOfActions = 0;
            SearchResponse scrollResponse = client.search(initialScroll);
            final String scrollId = scrollResponse.getScrollId();
            do {
                final SearchHit[] hits = scrollResponse.getHits().getHits();

                // Break condition: No hits are returned
                if (hits.length == 0) {
                    break;
                }

                for (SearchHit hit : hits) {
                    final DeleteRequest deleteRequest = new DeleteRequest(hit.getIndex(), hit.getType(), hit.getId());
                    bulkRequest.add(deleteRequest);
                    numberOfActions++;
                }

                final SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId)
                        .scroll(TimeValue.timeValueMinutes(1L));
                scrollResponse = client.searchScroll(searchScrollRequest);
                if (scrollResponse.getFailedShards() > 0) {
                    final String shardFailures = Arrays.toString(scrollResponse.getShardFailures());
                    throw new IllegalStateException("Error while fetching documents: " + shardFailures);
                }

            } while (true);

            if (numberOfActions > 0) {
                final BulkResponse bulkResponse = client.bulk(bulkRequest);
                if (bulkResponse.hasFailures()) {
                    throw new IllegalStateException("Error while deleting documents: " + bulkResponse.buildFailureMessage());
                }
            }

            final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollResponse.getScrollId());
            client.clearScroll(clearScrollRequest);

            refreshNode();
        }

    }

    private long documentCount() throws IOException {
        return RestClientHelper.count(client, Collections.emptySet(), Collections.emptySet());
    }

    private void refreshNode() throws IOException {
        RestClientHelper.refresh(client.getLowLevelClient(), Collections.emptySet());
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
    public RestHighLevelClient connectionManager() {
        return client;
    }
}
