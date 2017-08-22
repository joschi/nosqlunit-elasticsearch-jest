package com.github.joschi.nosqlunit.elasticsearch.jest.integration;

import com.github.joschi.nosqlunit.elasticsearch.jest.ElasticsearchRule;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Count;
import io.searchbox.core.CountResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.Refresh;
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
    private JestClient client;

    @Before
    public void setUp() throws IOException {
        for (String index : INDICES) {
            final Index indexRequest = new Index.Builder(Collections.singletonMap("msg", "Covfefe"))
                    .index(index)
                    .id(index)
                    .type("test")
                    .refresh(true)
                    .build();
            final DocumentResult documentResult = client.execute(indexRequest);
            assertThat(documentResult.getErrorMessage(), documentResult.isSucceeded(), is(true));
        }

        final JestResult refreshResult = client.execute(new Refresh.Builder().build());
        assertThat(refreshResult.getErrorMessage(), refreshResult.isSucceeded(), is(true));
    }

    @After
    public void tearDown() throws IOException {
        for (String index : INDICES) {
            final JestResult deleteTest = client.execute(new DeleteIndex.Builder(index).build());
            assertThat(deleteTest.getErrorMessage(), deleteTest.isSucceeded(), is(true));
        }

        final JestResult refreshResult = client.execute(new Refresh.Builder().build());
        assertThat(refreshResult.getErrorMessage(), refreshResult.isSucceeded(), is(true));
    }

    @Test
    @UsingDataSet(loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    public void testCleanInsert() throws Exception {
        final Count request = new Count.Builder().addIndex(INDICES).build();
        final CountResult result = client.execute(request);

        assertThat(result.getErrorMessage(), result.isSucceeded(), is(true));
        assertThat(result.getCount().intValue(), is(5));
    }

    @Test
    @UsingDataSet(loadStrategy = LoadStrategyEnum.INSERT)
    public void testInsert() throws Exception {
        final Count request = new Count.Builder().addIndex(INDICES).build();
        final CountResult result = client.execute(request);

        assertThat(result.getErrorMessage(), result.isSucceeded(), is(true));
        assertThat(result.getCount().intValue(), is(5));
    }

    @Test
    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    public void testDeleteAll() throws Exception {
        final Count request = new Count.Builder().addIndex(INDICES).ignoreUnavailable(true).build();
        final CountResult result = client.execute(request);

        assertThat(result.getErrorMessage(), result.isSucceeded(), is(true));
        assertThat(result.getCount().intValue(), is(2));
    }
}
