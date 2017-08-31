package com.github.joschi.nosqlunit.elasticsearch.http;

import io.searchbox.client.JestClient;

public interface ElasticsearchConnectionCallback {
    JestClient client();
}
