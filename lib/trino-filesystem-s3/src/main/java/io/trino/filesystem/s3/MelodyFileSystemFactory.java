//package io.trino.filesystem.s3;
//
//import com.google.inject.Inject;
//import hpe.harmony.sdk.java.api.HarmonyAwsCredentialsProvider;
//import io.airlift.units.DataSize;
//import io.trino.filesystem.TrinoFileSystem;
//import io.trino.filesystem.TrinoFileSystemFactory;
//import io.trino.spi.connector.ConnectorSession;
//import io.trino.spi.security.ConnectorIdentity;
//import software.amazon.awssdk.http.apache.ApacheHttpClient;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.S3ClientBuilder;
//
//import static java.lang.Math.toIntExact;
//
//public final class MelodyFileSystemFactory implements TrinoFileSystemFactory {
//
//    private final S3Client s3Client;
//    private final S3Context s3Context;
//
//    @Inject
//    public MelodyFileSystemFactory(S3FileSystemConfig cfg) {
//        S3FileSystemConfig config = new S3FileSystemConfig() {
//            @Override
//            public boolean isRequesterPays() {
//                return true;
//            }
//
//            @Override
//            public S3SseType getSseType() {
//                return S3SseType.S3;
//            }
//
//            @Override
//            public DataSize getStreamingPartSize() {
//                return DataSize.ofBytes(6 * 1024 * 1024);
//            }
//
//            @Override
//            public Integer getMaxConnections() {
//                return 3;
//            }
//        };
//        S3ClientBuilder s3 = S3Client.builder();
//
//        var cp = HarmonyAwsCredentialsProvider.builder()
//                .accessManagerEndpoint("https://dev.dataplatform.hpedev.net/dev/data-access-manager")
//                .oauthEndpoint("https://qa-sso.ccs.arubathena.com/as/token.oauth2")
//                .clientId("5c803829-bcff-48ea-9310-77e9c7a29a41_api")
//                .clientSecret("") // GLP client secret
//                .defaultOrg("octo")
//                .defaultDomain("harmony-dev")
//                .build();
//        s3.credentialsProvider(cp);
//
//        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
//                .maxConnections(config.getMaxConnections());
//
//        s3.httpClientBuilder(httpClient);
//
//        this.s3Client = s3.region(Region.US_WEST_2).build();
//
//        this.s3Context = new S3Context(
//                toIntExact(config.getStreamingPartSize().toBytes()),
//                config.isRequesterPays(),
//                config.getSseType(),
//                config.getSseKmsKeyId());
//    }
//
//    @Override
//    public TrinoFileSystem create(ConnectorIdentity identity) {
//        return null;
//    }
//
//    @Override
//    public TrinoFileSystem create(ConnectorSession session) {
//        return new S3FileSystem(s3Client, s3Context);
//    }
//}
