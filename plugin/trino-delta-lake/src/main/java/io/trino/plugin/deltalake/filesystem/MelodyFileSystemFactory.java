package io.trino.plugin.deltalake.filesystem;

import com.google.inject.Inject;
import hpe.harmony.model.scalalang.TemporaryCredentialsBuilder;
import hpe.harmony.model.scalalang.TokenExchangeBuilder;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.toIntExact;

public final class MelodyFileSystemFactory implements TrinoFileSystemFactory {

    private final S3Context s3Context;
    private final Map<String, MelodyFileSystem> fileSystems = new HashMap<>();

    private final TokenExchangeBuilder teb = new TokenExchangeBuilder();
    private final TemporaryCredentialsBuilder credentialsBuilder = new TemporaryCredentialsBuilder();
    private final CloseableHttpClient client = HttpClientBuilder.create().build();
    private static final Logger log = Logger.get(MelodyFileSystemFactory.class);
    private final S3FileSystemConfig s3Config;
    private final DeltaLakeConfig connectorConfig;
    private String t = ""; // TODO token from session

    @Inject
    public MelodyFileSystemFactory(S3FileSystemConfig s3Config, DeltaLakeConfig connectorConfig) {
        this.s3Config = s3Config;
        this.connectorConfig = connectorConfig;
        this.s3Context = new S3Context(
                toIntExact(s3Config.getStreamingPartSize().toBytes()),
                s3Config.isRequesterPays(),
                S3FileSystemConfig.S3SseType.S3,
                s3Config.getSseKmsKeyId(),
                s3Config.getMaxConnections());
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity) {
        throw new UnsupportedOperationException("MelodyFileSystemFactory requires org, domain, and token to create a file system");
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session) {
        throw new UnsupportedOperationException("MelodyFileSystemFactory requires org, domain, and token to create a file system");
    }

    public MelodyFileSystem getOrCreate(ConnectorSession session, String org, String domain) {
        String key = org + "/" + domain + "/" + t;

        MelodyFileSystem fs = fileSystems.get(key);

        if (fs == null) {
            fs = createFilesystem(org, domain, t);
            fileSystems.put(key, fs);
        } else if (fs.isCredsExpired()) { // TODO better check than just checking creds expiration
            log.info("Creds expired, rotating");
            fs.close();
            fs = createFilesystem(org, domain, t);
            fileSystems.put(key, fs);
        }
        return fs;
    }

    private MelodyFileSystem createFilesystem(String org, String domain, String token) {
        S3ClientBuilder builder = S3Client.builder();

        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
                .maxConnections(s3Context.maxConnections());

        var creds = generateTemporaryCredentials(org, domain, token);
        StaticCredentialsProvider scp = StaticCredentialsProvider.create(creds);

        builder.httpClient(httpClient.build());
        builder.credentialsProvider(scp);
        builder.region(Region.of(s3Config.getRegion()));

        return new MelodyFileSystem(builder.build(), s3Context, creds.expirationTime().orElse(Instant.now()));
    }

    private AwsSessionCredentials generateTemporaryCredentials(String org, String domain, String token) {
        var exchangeRequest = teb.buildExchangeRequestFor(domain, org);
        var json = teb.encode(exchangeRequest);

        String url = StringUtils.stripEnd(connectorConfig.getMelodyBaseUrl(), "/") + "/data-access-manager/credentials/exchange";
        var request = new HttpPost(url);

        request.addHeader("Content-Type", "application/json");
        request.addHeader("Authorization", "Bearer " + token);
        try {
            request.setEntity(new StringEntity(json));
            CloseableHttpResponse response = client.execute(request);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Unsuccessful response from AccessManager: " + response.getStatusLine().toString());
            }

            var jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
            var credentials = credentialsBuilder.decode(jsonResponse.toString());

            return AwsSessionCredentials.builder()
                    .accessKeyId(credentials.AccessKeyId())
                    .secretAccessKey(credentials.SecretAccessKey())
                    .sessionToken(credentials.SessionToken())
                    .expirationTime(Instant.parse(credentials.Expiration()))
                    .build();
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to encode payload to Access Manager");
            throw new RuntimeException(e);
        } catch (ClientProtocolException e) {
            log.error("Unexpected HTTP exception during request to Access Manager");
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("Unexpected IO exception during request to Access Manager");
            throw new RuntimeException(e);
        }
    }
}
