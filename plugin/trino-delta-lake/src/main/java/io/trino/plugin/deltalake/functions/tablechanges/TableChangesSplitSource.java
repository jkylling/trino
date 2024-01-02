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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.google.common.util.concurrent.Futures;
import io.airlift.log.Logger;
import io.trino.filesystem.Locations;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CdcEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.CDF_FILE;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.DATA_FILE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class TableChangesSplitSource
        implements ConnectorSplitSource
{
    private final String tableLocation;
    private final AsyncQueue<ConnectorSplit> queue;
    private static final Logger LOG = Logger.get(TableChangesSplitSource.class);
    private final ExecutorService executor;
    private volatile TrinoException trinoException;

    public TableChangesSplitSource(
            ConnectorSession session,
            TrinoFileSystemFactory fileSystemFactory,
            TableChangesTableFunctionHandle functionHandle,
            ExecutorService executorService,
            int maxSplitsPerSecond,
            int maxOutstandingSplits)
    {
        tableLocation = functionHandle.tableLocation();
        queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executorService);
        executor = Executors.newFixedThreadPool(32);
        prepareSplits(
                functionHandle.firstReadVersion(),
                functionHandle.tableReadVersion(),
                getTransactionLogDir(functionHandle.tableLocation()),
                fileSystemFactory.create(session),
                executor,
                executorService);
    }

    private CompletableFuture<Void> prepareSplits(long currentVersion, long tableReadVersion, String transactionLogDir, TrinoFileSystem fileSystem, ExecutorService executor, ExecutorService executorService)
    {
        return CompletableFuture.runAsync(() -> {
            try {
                CompletableFuture<?>[] futures = LongStream.range(currentVersion, tableReadVersion + 1)
                        .boxed()
                        .map(version -> CompletableFuture.runAsync(() -> {
                            try {
                                List<DeltaLakeTransactionLogEntry> entries = getEntriesFromJson(version, transactionLogDir, fileSystem)
                                        .orElseThrow(() -> new TrinoException(DELTA_LAKE_BAD_DATA, "Delta Lake log entries are missing for version " + version));
                                if (entries.isEmpty()) {
                                    return;
                                }
                                List<CommitInfoEntry> commitInfoEntries = entries.stream()
                                        .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                                        .filter(Objects::nonNull)
                                        .collect(toImmutableList());
                                if (commitInfoEntries.size() != 1) {
                                    throw new TrinoException(DELTA_LAKE_BAD_DATA, "There should be exactly 1 commitInfo present in a metadata file");
                                }
                                CommitInfoEntry commitInfo = getOnlyElement(commitInfoEntries);

                                List<ConnectorSplit> splits = new ArrayList<>();
                                boolean containsCdcEntry = false;
                                boolean containsRemoveEntry = false;
                                for (DeltaLakeTransactionLogEntry entry : entries) {
                                    CdcEntry cdcEntry = entry.getCDC();
                                    if (cdcEntry != null) {
                                        containsCdcEntry = true;
                                        splits.add(mapToDeltaLakeTableChangesSplit(
                                                commitInfo,
                                                CDF_FILE,
                                                cdcEntry.getSize(),
                                                cdcEntry.getPath(),
                                                cdcEntry.getCanonicalPartitionValues()));
                                    }
                                    if (entry.getRemove() != null && entry.getRemove().isDataChange()) {
                                        containsRemoveEntry = true;
                                    }
                                }
                                if (containsRemoveEntry && !containsCdcEntry) {
                                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Change Data Feed is not enabled at version %d. Version contains 'remove' entries without 'cdc' entries", version));
                                }
                                if (!containsRemoveEntry) {
                                    for (DeltaLakeTransactionLogEntry entry : entries) {
                                        if (entry.getAdd() != null && entry.getAdd().isDataChange()) {
                                            AddFileEntry addEntry = entry.getAdd();
                                            splits.add(mapToDeltaLakeTableChangesSplit(
                                                    commitInfo,
                                                    DATA_FILE,
                                                    addEntry.getSize(),
                                                    addEntry.getPath(),
                                                    addEntry.getCanonicalPartitionValues()));
                                        }
                                    }
                                }
                                splits.forEach(queue::offer);
                            }
                            catch (IOException e) {
                                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Failed to access table metadata", e);
                            }
                        }, executor)).toArray(CompletableFuture[]::new);
                CompletableFuture.allOf(futures)
                        .exceptionally(
                                throwable -> {
                                    if (throwable.getCause() instanceof TrinoException) {
                                        trinoException = (TrinoException) throwable.getCause();
                                    }
                                    else {
                                        trinoException = new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to compute table_changes", throwable);
                                    }
                                    try {
                                        // Finish the queue to wake up threads from queue.getBatchAsync()
                                        queue.finish();
                                    }
                                    catch (Exception e) {
                                        // if we can't finish the queue, consumers that might be waiting for more elements will remain blocked indefinitely
                                        LOG.error(e, "Failure");
                                    }
                                    return null;
                                })
                        .join();
                queue.finish();
            }
            finally {
                executor.shutdown();
            }
        }, executorService);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        boolean noMoreSplits = isFinished();
        if (trinoException != null) {
            return toCompletableFuture(immediateFailedFuture(trinoException));
        }
        return toCompletableFuture(Futures.transform(queue.getBatchAsync(maxSize), splits -> new ConnectorSplitBatch(splits, noMoreSplits), directExecutor()));
    }

    private TableChangesSplit mapToDeltaLakeTableChangesSplit(
            CommitInfoEntry commitInfoEntry,
            TableChangesFileType source,
            long length,
            String entryPath,
            Map<String, Optional<String>> canonicalPartitionValues)
    {
        String path = Locations.appendPath(tableLocation, entryPath);
        return new TableChangesSplit(
                path,
                length,
                canonicalPartitionValues,
                commitInfoEntry.getTimestamp(),
                source,
                commitInfoEntry.getVersion());
    }

    @Override
    public void close()
    {
        queue.finish();
        executor.shutdownNow();
    }

    @Override
    public boolean isFinished()
    {
        if (queue.isFinished()) {
            // Note: queue and trinoException need to be checked in the appropriate order
            // When throwable is set, we want getNextBatch to be called, so that we can propagate the exception
            return trinoException == null;
        }
        return false;
    }
}
