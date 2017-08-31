package com.github.joschi.nosqlunit.elasticsearch.http.integration;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Properties;

public abstract class BaseIT {
    private static final String PROPERTIES_RESOURCE_NAME = "es.properties";

    protected static String getServer() {
        return getServer(PROPERTIES_RESOURCE_NAME);
    }

    protected static String getServer(String resourceName) {
        final Properties properties = new Properties();
        final URL resource = Resources.getResource(resourceName);
        try (InputStream stream = resource.openStream()) {
            properties.load(stream);
        } catch (IOException e) {
            throw new UncheckedIOException("Error while reading test properties", e);
        }

        final String httpPort = properties.getProperty("httpPort", "9200");
        return String.format("http://localhost:%s/", httpPort);
    }
}
