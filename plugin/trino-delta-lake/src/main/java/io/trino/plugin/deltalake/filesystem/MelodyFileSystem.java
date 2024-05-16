/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake.filesystem;

import com.amazonaws.auth.SessionCredentialsProviderFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import hpe.harmony.model.scalalang.*;
import io.airlift.log.Logger;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Multimaps.toMultimap;
import static java.util.Objects.requireNonNull;

public final class MelodyFileSystem
        implements TrinoFileSystem
{
    private final S3Context context;
    private final RequestPayer requestPayer;
    private static final Logger log = Logger.get(MelodyFileSystem.class);

    private Map<String, S3Client> s3Clients = new HashMap<>();
    private final CloseableHttpClient client = HttpClientBuilder.create().build();

    private final TokenExchangeBuilder teb = new TokenExchangeBuilder();
    private final TemporaryCredentialsBuilder credentialsBuilder = new TemporaryCredentialsBuilder();

    public MelodyFileSystem(S3Context context)
    {
        this.context = requireNonNull(context, "context is null");
        this.requestPayer = context.requestPayer();
    }

    private S3Client getOrCreateClient(String token, String org, String domain) {
        // TODO remove
        String t = "";
        S3Client client = s3Clients.get(t);

        if (client == null) {
            client = createClient(t, org, domain);
            s3Clients.put(t, client);
        }
        return client;
    }

    private S3Client createClient(String token, String org, String domain) {
        S3ClientBuilder builder = S3Client.builder();


        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
                .maxConnections(3); // TODO configurable

        var creds = generateTemporaryCredentials(token, org, domain);
        StaticCredentialsProvider scp = StaticCredentialsProvider.create(creds);

        builder.httpClient(httpClient.build());
        builder.region(Region.US_WEST_2); // TODO is region needed?
        builder.credentialsProvider(scp);

        return builder.build();
    }

    private AwsSessionCredentials generateTemporaryCredentials(String token, String org, String domain) {
        var exchangeRequest = teb.buildExchangeRequestFor(domain, org);
        var json = teb.encode(exchangeRequest);

        // TODO configurable
        var request = new HttpPost("https://dev.dataplatform.hpedev.net/dev/data-access-manager/credentials/exchange");

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

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to get file handles");
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to get file handles");
    }

    public TrinoInputFile newInputFile(Location location, String org, String domain, String token)
    {
        return new S3InputFile(getOrCreateClient(token, org, domain), context, new S3Location(location), null);
    }

    public TrinoInputFile newInputFile(Location location, long length, String org, String domain, String token)
    {
        return new S3InputFile(getOrCreateClient(token, org, domain), context, new S3Location(location), length);
    }


    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to get file handles");
    }

    public TrinoOutputFile newOutputFile(Location location, String org, String domain, String token)
    {
        return new S3OutputFile(getOrCreateClient(token, org, domain), context, new S3Location(location));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to delete files");
    }

    @Override
    public void deleteDirectory(Location location) {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to delete directories");
    }

    public void deleteFile(Location location, String org, String domain, String token)
            throws IOException
    {
        location.verifyValidFileLocation();
        S3Location s3Location = new S3Location(location);
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .requestPayer(requestPayer)
                .key(s3Location.key())
                .bucket(s3Location.bucket())
                .build();

        try {
            getOrCreateClient(token, org, domain).deleteObject(request);
        }
        catch (SdkException e) {
            throw new IOException("Failed to delete file: " + location, e);
        }
    }

    public void deleteDirectory(Location location, String org, String domain, String token)
            throws IOException
    {
        FileIterator iterator = listFiles(location, org, domain, token);
        while (iterator.hasNext()) {
            List<Location> files = new ArrayList<>();
            while ((files.size() < 1000) && iterator.hasNext()) {
                files.add(iterator.next().location());
            }
            deleteFiles(files);
        }
    }

    public void deleteFiles(Collection<Location> locations, String org, String domain, String token)
            throws IOException
    {
        locations.forEach(Location::verifyValidFileLocation);

        SetMultimap<String, String> bucketToKeys = locations.stream()
                .map(S3Location::new)
                .collect(toMultimap(S3Location::bucket, S3Location::key, HashMultimap::create));

        Map<String, String> failures = new HashMap<>();

        for (Entry<String, Collection<String>> entry : bucketToKeys.asMap().entrySet()) {
            String bucket = entry.getKey();
            Collection<String> allKeys = entry.getValue();

            for (List<String> keys : partition(allKeys, 250)) {
                List<ObjectIdentifier> objects = keys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .toList();

                DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                        .requestPayer(requestPayer)
                        .bucket(bucket)
                        .delete(builder -> builder.objects(objects).quiet(true))
                        .build();

                try {
                    DeleteObjectsResponse response = getOrCreateClient(token, org, domain).deleteObjects(request);
                    for (S3Error error : response.errors()) {
                        failures.put("s3://%s/%s".formatted(bucket, error.key()), error.code());
                    }
                }
                catch (SdkException e) {
                    throw new IOException("Error while batch deleting files", e);
                }
            }
        }

        if (!failures.isEmpty()) {
            throw new IOException("Failed to delete one or more files: " + failures);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new IOException("S3 does not support renames");
    }

    @Override
    public FileIterator listFiles(Location location) throws IOException {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to get file handles");
    }

    @Override
    public Optional<Boolean> directoryExists(Location location) throws IOException {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to get file handles");
    }

    public FileIterator listFiles(Location location, String org, String domain, String token)
            throws IOException
    {
        S3Location s3Location = new S3Location(location);

        String key = s3Location.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(s3Location.bucket())
                .prefix(key)
                .build();

        try {
            ListObjectsV2Iterable iterable = getOrCreateClient(token, org, domain).listObjectsV2Paginator(request);
            return new S3FileIterator(s3Location, iterable.contents().iterator());
        }
        catch (SdkException e) {
            throw new IOException("Failed to list location: " + location, e);
        }
    }

    public Optional<Boolean> directoryExists(Location location, String org, String domain, String token)
            throws IOException
    {
        validateS3Location(location);
        if (location.path().isEmpty() || listFiles(location).hasNext()) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    @Override
    public void createDirectory(Location location)
    {
        validateS3Location(location);
        // S3 does not have directories
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new IOException("S3 does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location) throws IOException {
        throw new UnsupportedOperationException("MelodyFileSystem requires org, domain, and token to list directories");
    }

    public Set<Location> listDirectories(Location location, String org, String domain, String token)
            throws IOException
    {
        S3Location s3Location = new S3Location(location);
        Location baseLocation = s3Location.baseLocation();

        String key = s3Location.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(s3Location.bucket())
                .prefix(key)
                .delimiter("/")
                .build();

        try {
            return getOrCreateClient(token, org, domain).listObjectsV2Paginator(request)
                    .commonPrefixes().stream()
                    .map(CommonPrefix::prefix)
                    .map(baseLocation::appendPath)
                    .collect(toImmutableSet());
        }
        catch (SdkException e) {
            throw new IOException("Failed to list location: " + location, e);
        }
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateS3Location(Location location)
    {
        new S3Location(location);
    }
}
