package com.github.joschi.nosqlunit.elasticsearch.jest;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class ElasticsearchConfiguration extends AbstractJsr330Configuration {
    private final JestClient client;
    private final boolean deleteAllIndices;
    private final boolean createIndices;
    private final Map<String, Object> indexSettings;
    private final Map<String, Map<String, Object>> indexTemplates;

    ElasticsearchConfiguration(JestClient client,
                               boolean deleteAllIndices,
                               boolean createIndices,
                               Map<String, Object> indexSettings,
                               Map<String, Map<String, Object>> indexTemplates) {
        this.client = client;
        this.deleteAllIndices = deleteAllIndices;
        this.createIndices = createIndices;
        this.indexSettings = indexSettings;
        this.indexTemplates = indexTemplates;
    }

    public JestClient getClient() {
        return client;
    }

    public boolean isCreateIndices() {
        return createIndices;
    }

    public boolean isDeleteAllIndices() {
        return deleteAllIndices;
    }

    public Map<String, Object> getIndexSettings() {
        return indexSettings;
    }

    public Map<String, Map<String, Object>> getIndexTemplates() {
        return indexTemplates;
    }

    /**
     * Return a default builder for {@link ElasticsearchConfiguration}.
     */
    public static Builder remoteElasticsearch() {
        return new Builder();
    }

    /**
     * Return a builder for {@link ElasticsearchConfiguration} pre-configured with the given Elasticsearch node URL.
     *
     * @param server The URL of the Elasticsearch node to connect to
     */
    public static Builder remoteElasticsearch(String server) {
        return remoteElasticsearch(Collections.singleton(server));
    }

    /**
     * Return a builder for {@link ElasticsearchConfiguration} pre-configured with the given Elasticsearch node URLs.
     *
     * @param servers The URLs of the Elasticsearch nodes to connect to
     */
    public static Builder remoteElasticsearch(Set<String> servers) {
        return new Builder().servers(servers);
    }

    public static class Builder {
        private static final String DEFAULT_SERVER = "http://localhost:9200/";

        private Set<String> servers = Collections.singleton(DEFAULT_SERVER);
        private HttpClientConfig httpClientConfig = null;
        private boolean createIndices = false;
        private boolean deleteAllIndices = false;
        private Map<String, Object> indexSettings = Collections.emptyMap();
        private Map<String, Map<String, Object>> indexTemplates = Collections.emptyMap();

        private Builder() {
        }

        /**
         * The URL of the Elasticsearch node to connect to, for example {@code http://127.0.0.1:9200/}.
         *
         * @param server The URLs of the Elasticsearch nodes to connect to
         */
        public Builder server(String server) {
            return servers(Collections.singleton(server));
        }

        /**
         * The URLs of the Elasticsearch nodes to connect to, for example {@code http://127.0.0.1:9200/}.
         *
         * @param servers The URLs of the Elasticsearch nodes to connect to
         */
        public Builder servers(Set<String> servers) {
            this.servers = servers;
            return this;
        }

        /**
         * Set the HTTP client configuration for {@link JestClient}.
         */
        public Builder httpClientConfig(HttpClientConfig httpClientConfig) {
            this.httpClientConfig = httpClientConfig;
            return this;
        }

        /**
         * Whether to explicitly delete <em>all</em> indices from Elasticsearch when deleting data.
         *
         * @param deleteAllIndices Delete <em>all</em> indices from Elasticsearch if {@literal true}, delete individual documents if {@literal false}
         */
        public Builder deleteAllIndices(boolean deleteAllIndices) {
            this.deleteAllIndices = deleteAllIndices;
            return this;
        }

        /**
         * Whether to explicitly create Elasticsearch indices when inserting data.
         *
         * @see #indexSettings(Map)
         * @see #indexTemplates(Map)
         */
        public Builder createIndices(boolean createIndices) {
            this.createIndices = createIndices;
            return this;
        }

        /**
         * Index settings to use if explicitly creating indices is enabled when inserting data.
         *
         * @see #createIndices(boolean)
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_index_settings.html">Elasticsearch: The Definitive Guide » Getting Started » Index Management » Index Settings</a>
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html">Elasticsearch Reference » Index Modules</a>
         */
        public Builder indexSettings(Map<String, Object> indexSettings) {
            this.indexSettings = indexSettings;
            return this;
        }

        /**
         * Collection of index templates to create before inserting data.
         * The index templates will be deleted after data has been inserted.
         *
         * @param indexTemplates A map of index templates with the key being the name of the index template
         *                       and the value being the template source.
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html">Elasticsearch Reference » Indices APIs » Index Templates</a>
         */
        public Builder indexTemplates(Map<String, Map<String, Object>> indexTemplates) {
            this.indexTemplates = indexTemplates;
            return this;
        }

        public ElasticsearchConfiguration build() {
            final JestClient client = getClient();
            client.setServers(servers);

            return new ElasticsearchConfiguration(client, deleteAllIndices, createIndices, indexSettings, indexTemplates);
        }

        private JestClient getClient() {
            final JestClientFactory clientFactory = new JestClientFactory();
            if (httpClientConfig != null) {
                clientFactory.setHttpClientConfig(httpClientConfig);
            }
            return clientFactory.getObject();
        }
    }
}
