package io.trino.filesystem.melody;

import io.airlift.configuration.Config;

import java.net.URL;

public class MelodyFileSystemConfig {
    private URL melodyBaseUrl;
    private URL wellKnownUrl;
    private String clientId;
    private String clientSecret;

    public URL getMelodyBaseUrl() {
        return melodyBaseUrl;
    }

    @Config("melody.base-url")
    public MelodyFileSystemConfig setMelodyBaseUrl(URL melodyBaseUrl) {
        this.melodyBaseUrl = melodyBaseUrl;
        return this;
    }

    public URL getWellKnownUrl() {
        return wellKnownUrl;
    }

    @Config("melody.well-known-url")
    public void setWellKnownUrl(URL wellKnownUrl) {
        this.wellKnownUrl = wellKnownUrl;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    @Config("melody.credentials.secret")
    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getClientId() {
        return clientId;
    }

    @Config("melody.credentials.client-id")
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
