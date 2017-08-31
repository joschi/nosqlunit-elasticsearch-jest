package com.github.joschi.nosqlunit.elasticsearch.jest;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class RestClientHelper {
    public static long count(RestHighLevelClient client, Collection<String> indices, Collection<String> types) throws IOException {
        final SearchSourceBuilder source = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery())
                .size(0);
        final SearchRequest searchRequest = new SearchRequest(indices.toArray(new String[0]), source)
                .types(types.toArray(new String[0]));
        final SearchResponse searchResponse = client.search(searchRequest);

        return searchResponse.getHits().getTotalHits();
    }

    public static boolean refresh(RestClient client, Collection<String> indices) throws IOException {
        final String indicesList = indices.stream().collect(Collectors.joining(","));

        final String endpoint;
        if (indices.isEmpty()) {
            endpoint = "/_refresh";
        } else {
            endpoint = "/" + indicesList + "/_refresh";
        }

        final Response response = client.performRequest("POST", endpoint);
        return response.getStatusLine().getStatusCode() == 200;
    }

    public static boolean createIndex(RestClient client, String index, Map<String, Object> settings) throws IOException {
        final String endpoint = "/" + index;

        final String json = XContentFactory.jsonBuilder().map(settings).string();
        final HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);

        final Response response = client.performRequest("PUT", endpoint, Collections.emptyMap(), entity);
        return response.getStatusLine().getStatusCode() == 200;
    }

    public static boolean deleteIndex(RestClient client, Collection<String> indices) throws IOException {
        final String indicesList = indices.stream().collect(Collectors.joining(","));

        final String endpoint;
        if (indices.isEmpty()) {
            endpoint = "/_all";
        } else {
            endpoint = "/" + indicesList;
        }

        final Response response = client.performRequest("DELETE", endpoint);
        return response.getStatusLine().getStatusCode() == 200;
    }

    public static boolean createTemplate(RestClient client, String templateName, Map<String, Object> settings) throws IOException {
        final String endpoint = "/_template/" + templateName;

        final String json = XContentFactory.jsonBuilder().map(settings).string();
        final HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);

        final Response response = client.performRequest("PUT", endpoint, Collections.emptyMap(), entity);
        return response.getStatusLine().getStatusCode() == 200;
    }

    public static boolean deleteTemplate(RestClient client, String templateName) throws IOException {
        final String endpoint = "/_template/" + templateName;
        final Response response = client.performRequest("DELETE", endpoint);
        return response.getStatusLine().getStatusCode() == 200;
    }

    public static Map<String, Object> getSettings(RestClient client, Collection<String> indices) throws IOException {
        final String indicesList = indices.stream().collect(Collectors.joining(","));

        final String endpoint;
        if (indices.isEmpty()) {
            endpoint = "/_all/_settings";
        } else {
            endpoint = "/" + indicesList + "/_settings";
        }

        final Response response = client.performRequest("GET", endpoint);
        final HttpEntity responseEntity = response.getEntity();

        final XContentType xContentType = XContentType.fromMediaTypeOrFormat(responseEntity.getContentType().getValue());
        final XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY, responseEntity.getContent());

        return parser.map();
    }
}
