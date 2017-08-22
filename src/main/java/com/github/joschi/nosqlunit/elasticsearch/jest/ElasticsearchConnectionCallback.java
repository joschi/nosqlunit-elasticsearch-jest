package com.github.joschi.nosqlunit.elasticsearch.jest;

import io.searchbox.client.JestClient;

public interface ElasticsearchConnectionCallback {
    JestClient client();
}
