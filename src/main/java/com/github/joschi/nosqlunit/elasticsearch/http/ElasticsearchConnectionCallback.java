package com.github.joschi.nosqlunit.elasticsearch.http;

import org.elasticsearch.client.RestHighLevelClient;

public interface ElasticsearchConnectionCallback {
    RestHighLevelClient client();
}
