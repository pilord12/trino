package io.trino.hdfs.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import hpe.harmony.sdk.java.api.HarmonyAwsCredentialsProvider;

import com.amazonaws.auth.*;

import java.io.IOException;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import software.amazon.awssdk.utils.SdkAutoCloseable;

public class HarmonyAwsCredentialsProviderV1 implements AWSCredentialsProvider, SdkAutoCloseable {
    private static String HARMONY_CREDENTIALS_CLIENT_ID            = "fs.s3a.harmony.credentials.client.id";
    private static String  HARMONY_CREDENTIALS_CLIENT_SECRET       = "fs.s3a.harmony.credentials.client.secret";
    private static String  HARMONY_CREDENTIALS_TOKEN_ENDPOINT      = "fs.s3a.harmony.credentials.token.endpoint";
    private static String  HARMONY_CREDENTIALS_OAUTH_ENDPOINT      = "fs.s3a.harmony.credentials.oauth.endpoint";
    private static String  HARMONY_CREDENTIALS_ACCESS_MGR_ENDPOINT = "fs.s3a.harmony.credentials.am.endpoint";
    private static String  HARMONY_CONF                            = "harmony-conf";

    private CloseableHttpClient client = HttpClients.createDefault();

    @Override
    public void close() {
        synchronized(this) {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            delegate.close();
        }
    }

    protected HarmonyAwsCredentialsProvider delegate =
            HarmonyAwsCredentialsProvider
                    .builder()
            .clientId("5c803829-bcff-48ea-9310-77e9c7a29a41_api")
            .clientSecret("") // GLP client secret
            .oauthEndpoint("https://qa-sso.ccs.arubathena.com/as/token.oauth2")
            .accessManagerEndpoint("https://dev.dataplatform.hpedev.net/dev/data-access-manager")
            .defaultDomain("harmony-dev")
            .defaultOrg("octo")
            .client(client)
      .build();

    @Override
    public AWSCredentials getCredentials() {
        return resolveCreds();
    }

//    private def decodeConfig(input: String) =
//            try DsConf.loadFromJson(input)
//            catch {
//        case e: Throwable =>
//            logger.error(s"Unable to decode config: \n $input", e)
//            throw e
//    }

    private AWSCredentials resolveCreds() {
        try {
            var credv2 = delegate.resolveCredentials();
            return new BasicSessionCredentials(
                    credv2.accessKeyId(),
                    credv2.secretAccessKey(),
                    credv2.sessionToken()
            );
        } catch (Exception e) {
            throw new RuntimeException("unable to get AWS credentials", e);
        }
    }

    @Override
    public void refresh() {
        delegate.invalidateCreds();
    }
}
