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

import io.trino.Session;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDeltaLakeConcurrentWrites
        extends AbstractTestQueryFramework
{
    AtomicReference<Consumer<Location>> onNewInputFile = new AtomicReference<>((file) -> {});

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("delta_lake")
                .setSchema("default")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();
        try {
            String metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_metastore").toFile().getAbsoluteFile().toURI().toString();
            String writeMetastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_write_metastore").toFile().getAbsoluteFile().toURI().toString();

            queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.empty(), Optional.of(new HijackingTrinoFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS), onNewInputFile)), EMPTY_MODULE));
            queryRunner.createCatalog(
                    "delta_lake",
                    "delta_lake",
                    Map.of(
                            "hive.metastore", "file",
                            "hive.metastore.catalog.dir", metastoreDirectory,
                            "delta.register-table-procedure.enabled", "true",
                            "delta.enable-non-concurrent-writes", "true"));
            queryRunner.createCatalog(
                    "delta_lake_writer",
                    "delta_lake",
                    Map.of(
                            "hive.metastore", "file",
                            "hive.metastore.catalog.dir", writeMetastoreDirectory,
                            "delta.enable-non-concurrent-writes", "true"));

            queryRunner.execute("CREATE SCHEMA " + session.getSchema().orElseThrow());
            queryRunner.execute("CREATE SCHEMA delta_lake_writer." + session.getSchema().orElseThrow());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testOneQueryDivergentTableSnapshots()
    {
        String tableName = "test_divergent_snapshots";
        String schema = getSession().getSchema().orElseThrow();
        String writeTable = "delta_lake_writer." + schema + "." + tableName;

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + writeTable + "(c varchar)");
        assertUpdate("CALL delta_lake.system.register_table(schema_name => '" + schema + "', table_name => '" + tableName + "', table_location => '" + getTableLocation(writeTable) + "')");
        assertUpdate("INSERT INTO " + writeTable + " VALUES 'commit-1'", 1);

        AtomicInteger nextCommitCount = new AtomicInteger();
        onNewInputFile.set((file) -> {
            String commit = "00000000000000000002.json";
            nextCommitCount.addAndGet(file.path().endsWith(commit) ? 1 : 0);
            if (file.path().endsWith(commit) && nextCommitCount.get() == 5) { // simulate commit happening right before loading the TableSnapshot of the second table handle in the split manager
                onNewInputFile.set((x) -> {});
                assertUpdate("INSERT INTO " + writeTable + " VALUES ('commit-2')", 1);
            }
        });
        assertQuery("SELECT COUNT(*) FROM " + tableName + " UNION SELECT COUNT(*) FROM " + tableName, "VALUES 2, 1");
        onNewInputFile.set((file) -> {});
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    static class HijackingTrinoFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final TrinoFileSystemFactory delegate;
        private final AtomicReference<Consumer<Location>> onNewInputFile;

        HijackingTrinoFileSystemFactory(TrinoFileSystemFactory delegate, AtomicReference<Consumer<Location>> onNewInputFile)
        {
            this.delegate = delegate;
            this.onNewInputFile = onNewInputFile;
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            return new HijackingTrinoFileSystem(delegate.create(identity));
        }

        private class HijackingTrinoFileSystem
                implements TrinoFileSystem
        {
            private final TrinoFileSystem delegate;

            HijackingTrinoFileSystem(TrinoFileSystem delegate)
            {
                this.delegate = delegate;
            }

            @Override
            public TrinoInputFile newInputFile(Location location)
            {
                onNewInputFile.get().accept(location);
                return delegate.newInputFile(location);
            }

            @Override
            public TrinoInputFile newInputFile(Location location, long length)
            {
                onNewInputFile.get().accept(location);
                return delegate.newInputFile(location);
            }

            @Override
            public TrinoOutputFile newOutputFile(Location location)
            {
                return delegate.newOutputFile(location);
            }

            @Override
            public void deleteFile(Location location)
                    throws IOException
            {
                delegate.deleteFile(location);
            }

            @Override
            public void deleteDirectory(Location location)
                    throws IOException
            {
                delegate.deleteDirectory(location);
            }

            @Override
            public void renameFile(Location source, Location target)
                    throws IOException
            {
                delegate.renameFile(source, target);
            }

            @Override
            public FileIterator listFiles(Location location)
                    throws IOException
            {
                return delegate.listFiles(location);
            }

            @Override
            public Optional<Boolean> directoryExists(Location location)
                    throws IOException
            {
                return delegate.directoryExists(location);
            }
        }
    }
}
