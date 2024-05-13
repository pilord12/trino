package io.trino.filesystem.melody;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import okhttp3.*;
import org.json.JSONObject;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import static java.lang.Math.toIntExact;

public final class MelodyFileSystemFactory implements TrinoFileSystemFactory {

    private final S3Client s3Client;
    private final OkHttpClient httpClient;

    public MelodyFileSystemFactory(MelodyFileSystemConfig config) {
        httpClient = new OkHttpClient();

        String jsonString = requestTempCredentials(config.getMelodyBaseUrl());
        JSONObject jsonObject = new JSONObject(jsonString);
        TemporaryCredentials creds = new TemporaryCredentials(
            jsonObject.getString("accessKeyId"),
            jsonObject.getString("secretAccessKey"),
            jsonObject.getString("sessionToken"),
            jsonObject.getString("expiration"),
            jsonObject.getInt("version")
        );
        s3Client = null;

        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
                .maxConnections(config.getMaxConnections());

        if (config.getHttpProxy() != null) {
            URI endpoint = URI.create("%s://%s".formatted(
                    config.isHttpProxySecure() ? "https" : "http",
                    config.getHttpProxy()));
            httpClient.proxyConfiguration(ProxyConfiguration.builder()
                    .endpoint(endpoint)
                    .build());
        }

        s3.httpClientBuilder(httpClient);

        this.client = s3.build();

        context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                config.getSseType(),
                config.getSseKmsKeyId());
    }

    private String requestAccessToken(URL wellKnownUrl, String clientId, String secret) throws IOException {
        String creds = Credentials.basic(clientId, secret);
        Request request = new Request.Builder().url(wellKnownUrl).header("Authorization", creds).build();

        try (Response r = httpClient.newCall(request).execute()) {
            return r.body().string();
        } catch (IOException e) {
            throw e;
        }
    }

    private String requestTempCredentials(URL accessManagerUrl) {
        MediaType JSON = MediaType.get("application/json");

        OkHttpClient client = new OkHttpClient();

        RequestBody body = RequestBody.create("{json: \"adsfads\"}", JSON);
        Request request = new Request.Builder()
                .url(accessManagerUrl)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity) {
        return null;
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session) {
        return new S3FileSystem();
    }
}
