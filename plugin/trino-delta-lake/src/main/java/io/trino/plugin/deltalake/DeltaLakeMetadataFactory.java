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
package io.trino.plugin.deltalake;

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.MelodyFileSystemFactory;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.DeltaLakeTableStatisticsProvider;
import io.trino.plugin.deltalake.statistics.FileBasedTableStatisticsProvider;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.NodeManager;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class DeltaLakeMetadataFactory
{
    private final MelodyFileSystemFactory fileSystemFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final DeltaLakeAccessControlMetadataFactory accessControlMetadataFactory;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final TransactionLogWriterFactory transactionLogWriterFactory;
    private final NodeManager nodeManager;
    private final CheckpointWriterManager checkpointWriterManager;
    private final DeltaLakeRedirectionsProvider deltaLakeRedirectionsProvider;
    private final CachingExtendedStatisticsAccess statisticsAccess;
    private final int domainCompactionThreshold;
    private final boolean unsafeWritesEnabled;
    private final long checkpointWritingInterval;
    private final long perTransactionMetastoreCacheMaximumSize;
    private final boolean deleteSchemaLocationsFallback;
    private final boolean useUniqueTableLocation;
    private final DeltaLakeConfig config;

    private final boolean allowManagedTableRename;
    private final String trinoVersion;

    @Inject
    public DeltaLakeMetadataFactory(
            MelodyFileSystemFactory fileSystemFactory,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager,
            DeltaLakeAccessControlMetadataFactory accessControlMetadataFactory,
            DeltaLakeConfig deltaLakeConfig,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            TransactionLogWriterFactory transactionLogWriterFactory,
            NodeManager nodeManager,
            CheckpointWriterManager checkpointWriterManager,
            DeltaLakeRedirectionsProvider deltaLakeRedirectionsProvider,
            CachingExtendedStatisticsAccess statisticsAccess,
            @AllowDeltaLakeManagedTableRename boolean allowManagedTableRename,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.transactionLogWriterFactory = requireNonNull(transactionLogWriterFactory, "transactionLogWriterFactory is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.checkpointWriterManager = requireNonNull(checkpointWriterManager, "checkpointWriterManager is null");
        this.deltaLakeRedirectionsProvider = requireNonNull(deltaLakeRedirectionsProvider, "deltaLakeRedirectionsProvider is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.unsafeWritesEnabled = deltaLakeConfig.getUnsafeWritesEnabled();
        this.checkpointWritingInterval = deltaLakeConfig.getDefaultCheckpointWritingInterval();
        this.perTransactionMetastoreCacheMaximumSize = deltaLakeConfig.getPerTransactionMetastoreCacheMaximumSize();
        this.deleteSchemaLocationsFallback = deltaLakeConfig.isDeleteSchemaLocationsFallback();
        this.useUniqueTableLocation = deltaLakeConfig.isUniqueTableLocation();
        this.allowManagedTableRename = allowManagedTableRename;
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.config = deltaLakeConfig;
    }

    public DeltaLakeMetadata create(ConnectorIdentity identity)
    {
        MelodyDeltaLakeMetastore deltaLakeMetastore = new MelodyDeltaLakeMetastore();
        FileBasedTableStatisticsProvider tableStatisticsProvider = new FileBasedTableStatisticsProvider(
                typeManager,
                transactionLogAccess,
                statisticsAccess);
        return new DeltaLakeMetadata(
                deltaLakeMetastore,
                transactionLogAccess,
                tableStatisticsProvider,
                fileSystemFactory,
                typeManager,
                domainCompactionThreshold,
                unsafeWritesEnabled,
                dataFileInfoCodec,
                mergeResultJsonCodec,
                transactionLogWriterFactory,
                nodeManager,
                checkpointWriterManager,
                checkpointWritingInterval,
                deleteSchemaLocationsFallback,
                deltaLakeRedirectionsProvider,
                statisticsAccess,
                useUniqueTableLocation,
                allowManagedTableRename,
                config);
    }
}
