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
package io.trino.plugin.hive.ion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.WriterKind;
import io.trino.spi.Page;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.ION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.projectedColumn;
import static io.trino.plugin.hive.HiveTestUtils.toHiveBaseColumnHandle;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.ion.IonWriterOptions.BINARY_ENCODING;
import static io.trino.plugin.hive.ion.IonWriterOptions.ION_ENCODING_PROPERTY;
import static io.trino.plugin.hive.ion.IonWriterOptions.TEXT_ENCODING;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.stream.Collectors.toList;

/**
 * Most basic test to reflect PageSource-fu is wired up correctly.
 */
public class IonPageSourceSmokeTest
{
    // In the Ion binary format, a value stream is always start with binary version marker. This help distinguish Ion binary
    // data from other formats, including Ion text format.
    private static final byte[] BINARY_VERSION_MARKER = {(byte) 0xE0, (byte) 0x01, (byte) 0x00, (byte) 0XEA};
    private static final String EXPECTED_TEXT = "{foo:3,bar:6}";

    public static final String TEST_ION_LOCATION = "memory:///test.ion";

    @Test
    public void testReadTwoValues()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", VARCHAR, 1));

        assertRowCount(
                tableColumns,
                tableColumns,
                "{ foo: 31, bar: baz } { foo: 31, bar: \"baz\" }",
                2);
    }

    @Test
    public void testReadArray()
            throws IOException
    {
        List<HiveColumnHandle> tablesColumns = List.of(
                toHiveBaseColumnHandle("my_seq", new ArrayType(BOOLEAN), 0));

        assertRowCount(
                tablesColumns,
                tablesColumns,
                "{ my_seq: ( true false ) } { my_seq: [false, false, true] }",
                2);
    }

    @Test
    public void testProjectedColumn()
            throws IOException
    {
        final RowType spamType = RowType.rowType(field("nested_to_prune", INTEGER), field("eggs", INTEGER));
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("spam", spamType, 0),
                toHiveBaseColumnHandle("ham", BOOLEAN, 1));
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(0), "eggs"));

        assertRowCount(
                tableColumns,
                projectedColumns,
                // the data below reflects that "ham" is not decoded, that column is pruned
                // "nested_to_prune" is decoded, however, because nested fields are not pruned, yet.
                // so this test will fail if you change that to something other than an int
                "{ spam: { nested_to_prune: 31, eggs: 12 }, ham: exploding }",
                1);
    }

    @Test
    public void testPageSourceWithNativeTrinoDisabled()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", VARCHAR, 1));

        TestFixture fixture = new TestFixture(tableColumns)
                .withNativeIonDisabled();
        fixture.writeIonTextFile("{ foo: 31, bar: baz } { foo: 31, bar: \"baz\" }");

        Optional<ConnectorPageSource> connectorPageSource = fixture.getOptionalPageSource();
        Assertions.assertTrue(connectorPageSource.isEmpty(), "Expected empty page source when native Trino is disabled");
    }

    @Test
    public void testTextEncoding()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", INTEGER, 0));

        assertEncoding(tableColumns, TEXT_ENCODING);
    }

    @Test
    public void testBinaryEncoding()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", INTEGER, 0));

        assertEncoding(tableColumns, BINARY_ENCODING);
    }

    private void assertEncoding(List<HiveColumnHandle> tableColumns,
            String encoding)
            throws IOException
    {
        TestFixture fixture = new TestFixture(tableColumns)
                .withEncoding(encoding);

        writeTestData(fixture.getFileWriter());
        byte[] inputStreamBytes = fixture.getTrinoInputFile()
                .newStream()
                .readAllBytes();

        if (encoding.equals(BINARY_ENCODING)) {
            // Check if the first 4 bytes is binary version marker
            Assertions.assertArrayEquals(Arrays.copyOfRange(inputStreamBytes, 0, 4), BINARY_VERSION_MARKER);
        }
        else {
            Assertions.assertEquals(new String(inputStreamBytes, StandardCharsets.UTF_8), EXPECTED_TEXT);
        }
    }

    private void assertRowCount(List<HiveColumnHandle> tableColumns, List<HiveColumnHandle> projectedColumns, String ionText, int rowCount)
            throws IOException
    {
        TestFixture fixture = new TestFixture(tableColumns, projectedColumns);
        fixture.writeIonTextFile(ionText);

        try (ConnectorPageSource pageSource = fixture.getPageSource()) {
            final MaterializedResult result = MaterializedResult.materializeSourceDataStream(
                    fixture.getSession(),
                    pageSource,
                    projectedColumns.stream().map(HiveColumnHandle::getType).toList());
            Assertions.assertEquals(rowCount, result.getRowCount());
        }
    }

    private static void writeTestData(FileWriter ionFileWriter)
    {
        ionFileWriter.appendRows(new Page(
                RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {3}), 1),
                RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {6}), 1)));
        ionFileWriter.commit();
    }

    private static class TestFixture
    {
        private TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        private Location fileLocation = Location.of(TEST_ION_LOCATION);
        private HiveConfig hiveConfig = new HiveConfig();
        private Map<String, String> tableProperties = new HashMap<>();
        private List<HiveColumnHandle> columns;
        private List<HiveColumnHandle> projections;

        private ConnectorSession session;

        TestFixture(List<HiveColumnHandle> columns)
        {
            this(columns, columns);
        }

        TestFixture(List<HiveColumnHandle> columns, List<HiveColumnHandle> projections)
        {
            this.columns = columns;
            this.projections = projections;
            tableProperties.put(LIST_COLUMNS, columns.stream()
                    .map(HiveColumnHandle::getName)
                    .collect(Collectors.joining(",")));
            tableProperties.put(LIST_COLUMN_TYPES, columns.stream().map(HiveColumnHandle::getHiveType)
                    .map(HiveType::toString)
                    .collect(Collectors.joining(",")));
            // the default at runtime is false, but most of our testing obviously assumes it is enabled.
            hiveConfig.setIonNativeTrinoEnabled(true);
        }

        TestFixture withEncoding(String encoding)
        {
            tableProperties.put(ION_ENCODING_PROPERTY, encoding);
            return this;
        }

        TestFixture withNativeIonDisabled()
        {
            hiveConfig.setIonNativeTrinoEnabled(false);
            return this;
        }

        Optional<ConnectorPageSource> getOptionalPageSource()
                throws IOException
        {
            IonPageSourceFactory pageSourceFactory = new IonPageSourceFactory(fileSystemFactory, hiveConfig);

            long length = fileSystemFactory.create(getSession()).newInputFile(fileLocation).length();
            long nowMillis = Instant.now().toEpochMilli();

            List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                    "",
                    ImmutableList.of(),
                    projections,
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    fileLocation.toString(),
                    OptionalInt.empty(),
                    length,
                    nowMillis);

            return HivePageSourceProvider.createHivePageSource(
                    ImmutableSet.of(pageSourceFactory),
                    getSession(),
                    fileLocation,
                    OptionalInt.empty(),
                    0,
                    length,
                    length,
                    nowMillis,
                    new Schema(ION.getSerde(), false, tableProperties),
                    TupleDomain.all(),
                    TESTING_TYPE_MANAGER,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    NO_ACID_TRANSACTION,
                    columnMappings);
        }

        ConnectorPageSource getPageSource()
                throws IOException
        {
            return getOptionalPageSource().orElseThrow();
        }

        ConnectorSession getSession()
        {
            if (session == null) {
                session = getHiveSession(hiveConfig);
            }
            return session;
        }

        int writeIonTextFile(String ionText)
                throws IOException
        {
            final TrinoOutputFile outputFile = fileSystemFactory.create(getSession()).newOutputFile(fileLocation);
            int written = 0;
            try (OutputStream outputStream = outputFile.create()) {
                byte[] bytes = ionText.getBytes(StandardCharsets.UTF_8);
                outputStream.write(bytes);
                outputStream.flush();
                written = bytes.length;
            }
            return written;
        }

        FileWriter getFileWriter()
        {
            return new IonFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER)
                    .createFileWriter(
                            fileLocation,
                            columns.stream().map(HiveColumnHandle::getName).collect(toList()),
                            ION.toStorageFormat(),
                            HiveCompressionCodec.NONE,
                            tableProperties,
                            getSession(),
                            OptionalInt.empty(),
                            NO_ACID_TRANSACTION,
                            false,
                            WriterKind.INSERT)
                    .orElseThrow();
        }

        TrinoInputFile getTrinoInputFile()
        {
            return fileSystemFactory.create(getSession())
                    .newInputFile(fileLocation);
        }
    }
}
