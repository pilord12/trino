//package io.trino.plugin.deltalake;
//
//import hpe.harmony.sdk.java.api.HarmonyAwsCredentialsProvider;
//import io.trino.filesystem.TrinoFileSystem;
//import io.trino.filesystem.TrinoFileSystemFactory;
//import io.trino.filesystem.s3.S3Context;
//import io.trino.filesystem.s3.S3FileSystem;
//import io.trino.filesystem.s3.S3FileSystemConfig;
//import io.trino.spi.connector.ConnectorSession;
//import io.trino.spi.security.ConnectorIdentity;
//import software.amazon.awssdk.http.apache.ApacheHttpClient;
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
//    public MelodyFileSystemFactory(S3FileSystemConfig config) {
//        S3ClientBuilder s3 = S3Client.builder();
//
//        s3.credentialsProvider(HarmonyAwsCredentialsProvider.builder().build());
//
//        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
//                .maxConnections(config.getMaxConnections());
//
//        s3.httpClientBuilder(httpClient);
//
//        this.s3Client = s3.build();
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
