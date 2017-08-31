package com.github.joschi.nosqlunit.elasticsearch.http.integration;

import com.github.joschi.nosqlunit.elasticsearch.http.ElasticsearchRule;
import com.github.joschi.nosqlunit.elasticsearch.http.RestClientHelper;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ElasticsearchRuleIT extends BaseIT {
    private static final Collection<String> INDICES = Arrays.asList("test1", "test2");

    @Rule
    public final ElasticsearchRule elasticsearchRule = ElasticsearchRule.newElasticsearchRule().remoteElasticsearch(getServer());

    @Inject
    private RestHighLevelClient client;

    @Before
    public void setUp() throws IOException {
        for (String index : INDICES) {
            final IndexRequest indexRequest = new IndexRequest(index, "test", index)
                    .source(Collections.singletonMap("msg", "Covfefe"));
            final IndexResponse indexResponse = client.index(indexRequest);
        }

        RestClientHelper.refresh(client.getLowLevelClient(), Collections.emptySet());
    }

    @After
    public void tearDown() throws IOException {
        RestClientHelper.deleteIndex(client.getLowLevelClient(), INDICES);
        RestClientHelper.refresh(client.getLowLevelClient(), Collections.emptySet());
    }

    @Test
    @UsingDataSet(loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    public void testCleanInsert() throws Exception {
        final long count = RestClientHelper.count(client, INDICES, Collections.emptySet());

        assertThat(count, is(5L));
    }

    @Test
    @UsingDataSet(loadStrategy = LoadStrategyEnum.INSERT)
    public void testInsert() throws Exception {
        final long count = RestClientHelper.count(client, INDICES, Collections.emptySet());

        assertThat(count, is(5L));
    }

    @Test
    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    public void testDeleteAll() throws Exception {
        final long count = RestClientHelper.count(client, INDICES, Collections.emptySet());

        assertThat(count, is(2L));
    }
}
