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
 * More comprehensive tests, with assertions on values are in the TestIonFormat test
 * in the lib/trino-hive sub-module.
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
    public void testNestedProjection()
            throws IOException
    {
        final RowType spamType = RowType.rowType(field("nested_to_prune", INTEGER), field("eggs", INTEGER));
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("spam", spamType, 0),
                toHiveBaseColumnHandle("ham", BOOLEAN, 1));
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(0), "eggs"));

        assertRowValues(new TestFixture(tableColumns, projectedColumns),
                // decoding either 'ham' or 'nested_to_prune' would result in an error
                "{ spam: { nested_to_prune: splodysplode, eggs: 12 }, ham: exploding }",
                List.of(12));
    }

    @Test
    public void testDereferenceTwoNestedSiblings()
            throws IOException
    {
        final RowType spamType = RowType.rowType(field("cans", INTEGER), field("eggs", INTEGER));
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("spam", spamType, 0),
                toHiveBaseColumnHandle("ham", BOOLEAN, 1));
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(0), "eggs"),
                projectedColumn(tableColumns.get(0), "cans"));

        assertRowValues(new TestFixture(tableColumns, projectedColumns),
                "{ spam: { cans: 42, eggs: 12 }, ham: exploding }",
                List.of(12, 42));
    }

    @Test
    public void testBasicPathExtraction()
            throws IOException
    {
        // equivalent to a projection of "spam.eggs" from the query
        // and a path extraction of "meats.ham" from the serde
        final RowType spamType = RowType.rowType(field("eggs", INTEGER));
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("spam", spamType, 0),
                toHiveBaseColumnHandle("ham", BOOLEAN, 1));
        List<HiveColumnHandle> projections = List.of(
                projectedColumn(tableColumns.get(0), "eggs"),
                tableColumns.get(1));

        TestFixture fixture = new TestFixture(tableColumns, projections)
                .withPathExtractor("ham", "(meats ham)");

        assertRowValues(
                fixture,
                "{ spam: { eggs: 12 }, meats: { ham: true } }",
                List.of(12, true));
    }

    @Test
    public void testPathExtractionWithIndexedListItems()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("cell", VARCHAR, 0),
                toHiveBaseColumnHandle("home", VARCHAR, 1));

        TestFixture fixture = new TestFixture(tableColumns)
                .withPathExtractor("cell", "(phone 0)")
                .withPathExtractor("home", "(phone 2)");

        assertRowValues(
                fixture,
                "{ phone: ['999-555-1212', '999-999-9999', '999-555-3434'] }",
                List.of("999-555-1212", "999-555-3434"));
    }

    @Test
    public void testPathExtractTopLevelValue()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("single_col", INTEGER, 0));

        // the '()' path extractor extracts any top-level value, whether struct or not
        TestFixture fixture = new TestFixture(tableColumns)
                .withPathExtractor("single_col", "()");

        assertRowValues(
                fixture,
                "17 31 53",
                List.of(17), List.of(31), List.of(53));
    }

    @Test
    public void testNoProjectedColumns()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", VARCHAR, 0));

        TestFixture fixture = new TestFixture(tableColumns, List.of());

        assertRowCount(
                fixture,
                "46 { foo: baz } false",
                3);
    }

    @Test
    public void testNoProjectedColumnsWithTopLevelCapture()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("single_col", INTEGER, 0));

        TestFixture fixture = new TestFixture(tableColumns, List.of())
                .withPathExtractor("single_col", "()");

        assertRowCount(
                fixture,
                "46 { foo: baz } false",
                3);
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
        assertRowCount(fixture, ionText, rowCount);
    }

    private void assertRowCount(TestFixture fixture, String ionText, int rowCount)
            throws IOException
    {
        fixture.writeIonTextFile(ionText);

        try (ConnectorPageSource pageSource = fixture.getPageSource()) {
            final MaterializedResult result = MaterializedResult.materializeSourceDataStream(
                    fixture.getSession(),
                    pageSource,
                    fixture.projections.stream().map(HiveColumnHandle::getType).toList());
            Assertions.assertEquals(rowCount, result.getRowCount());
        }
    }

    void assertRowValues(TestFixture fixture, String ionText, List<Object>... values)
            throws IOException
    {
        fixture.writeIonTextFile(ionText);

        try (ConnectorPageSource pageSource = fixture.getPageSource()) {
            final MaterializedResult result = MaterializedResult.materializeSourceDataStream(
                    fixture.getSession(),
                    pageSource,
                    fixture.projections.stream().map(HiveColumnHandle::getType).toList());
            Assertions.assertEquals(values.length, result.getRowCount());
            for (int i = 0; i < values.length; i++) {
                Assertions.assertEquals(values[i], result.getMaterializedRows().get(i).getFields());
            }
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

        TestFixture withPathExtractor(String column, String pathExtraction)
        {
            tableProperties.put("ion.%s.path_extractor".formatted(column), pathExtraction);
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
