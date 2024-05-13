package io.trino.filesystem.melody;

public class TemporaryCredentials {
    String accessKeyId;
    String secretAccessKey;
    String sessionToken;
    String expiration;
    int version;

    public TemporaryCredentials(String accessKeyId, String secretAccessKey, String sessionToken, String expiration, int version) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.expiration = expiration;
        this.version = version;
    }
}
