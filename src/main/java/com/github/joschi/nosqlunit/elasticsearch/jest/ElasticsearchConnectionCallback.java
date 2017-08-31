package com.github.joschi.nosqlunit.elasticsearch.jest;

import org.elasticsearch.client.RestHighLevelClient;

public interface ElasticsearchConnectionCallback {
    RestHighLevelClient client();
}
