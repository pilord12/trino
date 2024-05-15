package io.trino.plugin.deltalake.filesystem;

import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3Context;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import static java.lang.Math.toIntExact;

public final class MelodyFileSystemFactory implements TrinoFileSystemFactory {

    private final S3Context s3Context;

    @Inject
    public MelodyFileSystemFactory() {
        S3FileSystemConfig config = new S3FileSystemConfig() {
            @Override
            public boolean isRequesterPays() {
                return true;
            }

            @Override
            public S3SseType getSseType() {
                return S3SseType.S3;
            }

            @Override
            public DataSize getStreamingPartSize() {
                return DataSize.ofBytes(6 * 1024 * 1024);
            }

            @Override
            public Integer getMaxConnections() {
                return 3;
            }
        };

        this.s3Context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                config.getSseType(),
                config.getSseKmsKeyId());
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity) {
        return null;
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session) {
        return new MelodyFileSystem(s3Context);
    }
}
