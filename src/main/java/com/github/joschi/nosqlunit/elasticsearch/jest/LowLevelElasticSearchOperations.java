package com.github.joschi.nosqlunit.elasticsearch.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.State;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LowLevelElasticSearchOperations {
    private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;

    public static void assertThatConnectionToElasticsearchIsPossible(String server) {
        assertThatConnectionToElasticsearchIsPossible(server, NUM_RETRIES_TO_CHECK_SERVER_UP);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(Set<String> servers) {
        assertThatConnectionToElasticsearchIsPossible(servers, NUM_RETRIES_TO_CHECK_SERVER_UP);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(String server, int numRetries) {
        assertThatConnectionToElasticsearchIsPossible(Collections.singleton(server), NUM_RETRIES_TO_CHECK_SERVER_UP, numRetries, TimeUnit.SECONDS);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(Set<String> servers, int numRetries) {
        assertThatConnectionToElasticsearchIsPossible(servers, NUM_RETRIES_TO_CHECK_SERVER_UP, numRetries, TimeUnit.SECONDS);
    }

    public static void assertThatConnectionToElasticsearchIsPossible(Set<String> servers, int numRetries, int waitTime, TimeUnit waitUnit) {
        final JestClientFactory clientFactory = new JestClientFactory();
        final JestClient jestClient = clientFactory.getObject();
        try {
            jestClient.setServers(servers);
            for (int i = 0; i < numRetries; i++) {
                try {
                    final State request = new State.Builder().build();
                    final JestResult result = jestClient.execute(request);

                    if (result.isSucceeded()) {
                        return;
                    }
                } catch (Exception e) {
                    Uninterruptibles.sleepUninterruptibly(waitTime, waitUnit);
                }
            }
        } finally {
            if (jestClient != null) {
                jestClient.shutdownClient();
            }
        }

        throw new AssertionError("Couldn't connect to Elasticsearch at " + servers);
    }
}
