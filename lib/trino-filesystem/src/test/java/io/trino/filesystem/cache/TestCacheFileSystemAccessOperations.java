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
package io.trino.filesystem.cache;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.slice.Slices;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.filesystem.tracing.InputFileMethod;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.block.TestingSession;
import io.trino.util.AutoCloseableCloser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.filesystem.tracing.InputFileMethod.INPUT_FILE_LAST_MODIFIED;
import static io.trino.filesystem.tracing.InputFileMethod.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.tracing.InputFileMethod.fromMethodName;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestCacheFileSystemAccessOperations
{
    public static final String TRACE_PREFIX = "InputFile.";
    private CacheFileSystem fileSystem;
    private InMemorySpanExporter spanExporter;
    private AutoCloseableCloser closer;

    @BeforeAll
    void setUp()
    {
        closer = AutoCloseableCloser.create();
        spanExporter = closer.register(InMemorySpanExporter.create());
        SdkTracerProvider tracerProvider = closer.register(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build());
        TracingFileSystemFactory tracingFileSystemFactory = new TracingFileSystemFactory(tracerProvider.get("test-cache-filesystem"), new MemoryFileSystemFactory());
        fileSystem = new CacheFileSystem(tracingFileSystemFactory.create(TestingSession.SESSION), new TestingMemoryFileSystemCache(), new DefaultCacheKeyProvider());
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        closer.close();
        fileSystem = null;
    }

    @Test
    void testCache()
            throws IOException
    {
        Location location = getRootLocation().appendPath("hello");
        byte[] content = "hello world".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        assertReadOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build());
        assertReadOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build());

        byte[] modifiedContent = "modified content".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).createOrOverwrite()) {
            output.write(modifiedContent);
        }

        assertReadOperations(location, modifiedContent,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build());
    }

    private Location getRootLocation()
    {
        return Location.of("memory://");
    }

    private void assertReadOperations(Location location, byte[] content, Multiset<FileOperation> fileOperations)
            throws IOException
    {
        TrinoInputFile file = fileSystem.newInputFile(location);
        int length = (int) file.length();
        spanExporter.reset();
        try (TrinoInput input = file.newInput()) {
            assertThat(input.readFully(0, length)).isEqualTo(Slices.wrappedBuffer(content));
        }
        assertMultisetsEqual(fileOperations, getOperations());
    }

    private Multiset<FileOperation> getOperations()
    {
        return spanExporter.getFinishedSpanItems().stream()
                .filter(span -> span.getName().startsWith(TRACE_PREFIX))
                .map(span -> new FileOperation(
                        Location.of(span.getAttributes().get(FILE_LOCATION)),
                        fromMethodName(span.getName())))
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(Location path, InputFileMethod operationType) {}
}
