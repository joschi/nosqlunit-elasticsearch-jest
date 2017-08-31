package com.github.joschi.nosqlunit.elasticsearch.http;

import org.apache.http.HttpHost;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LowLevelElasticSearchOperations {
    private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;

    public static void assertThatConnectionToElasticsearchIsPossible(HttpHost server) {
        assertThatConnectionToElasticsearchIsPossible(server, NUM_RETRIES_TO_CHECK_SERVER_UP);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(Set<HttpHost> servers) {
        assertThatConnectionToElasticsearchIsPossible(servers, NUM_RETRIES_TO_CHECK_SERVER_UP);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(HttpHost server, int numRetries) {
        assertThatConnectionToElasticsearchIsPossible(Collections.singleton(server), NUM_RETRIES_TO_CHECK_SERVER_UP, numRetries, TimeUnit.SECONDS);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(Set<HttpHost> servers, int numRetries) {
        assertThatConnectionToElasticsearchIsPossible(servers, NUM_RETRIES_TO_CHECK_SERVER_UP, numRetries, TimeUnit.SECONDS);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(Collection<HttpHost> servers, int numRetries, int waitTime, TimeUnit waitUnit) {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(servers.toArray(new HttpHost[0])))) {
            for (int i = 0; i < numRetries; i++) {
                try {
                    final MainResponse info = client.info();
                    if (info.isAvailable()) {
                        return;
                    }
                } catch (Exception e) {
                    Uninterruptibles.sleepUninterruptibly(waitTime, waitUnit);
                }
            }
        } catch (IOException e) {
            throw new AssertionError("Couldn't connect to Elasticsearch at " + servers, e);
        }

        throw new AssertionError("Couldn't connect to Elasticsearch at " + servers);
    }
}
