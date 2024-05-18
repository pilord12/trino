package io.trino.plugin.deltalake.filesystem;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.toIntExact;

public final class MelodyFileSystemFactory implements TrinoFileSystemFactory {

    private final S3Context s3Context;
    private final Map<String, MelodyFileSystem> fileSystems = new HashMap<>();

    @Inject
    public MelodyFileSystemFactory(S3FileSystemConfig config) {
        this.s3Context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                S3FileSystemConfig.S3SseType.S3,
                config.getSseKmsKeyId(),
                config.getMaxConnections());
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity) {
        return null;
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session) {
        String token = ""; // TODO token from session

        MelodyFileSystem fs = fileSystems.get(token);

        if (fs == null) {
            fs = new MelodyFileSystem(s3Context);
            fileSystems.put(token, fs);
        }
        return fs;
    }
}
