package io.trino.plugin.hive.ion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.util.HiveTypeTranslator;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.RowType;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.ION;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.TestHiveFileFormats.testPageSourceFactory;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;

/**
 * Most basic test to reflect PageSource-fu is wired up correctly.
 */
public class IonPageSourceSmokeTest
{

    @Test
    public void testReadTwoValues()
            throws IOException
    {
        HiveColumnHandle foo = new HiveColumnHandle("foo", 0, HiveType.HIVE_INT, INTEGER, Optional.empty(), HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        HiveColumnHandle bar = new HiveColumnHandle("bar", 1, HiveType.HIVE_STRING, VARCHAR, Optional.empty(), HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        List<HiveColumnHandle> tableColumns = ImmutableList.of(foo, bar);

        assertRowCount(
                tableColumns,
                tableColumns,
                "{ foo: 31, bar: baz } { foo: 31, bar: \"baz\" }",
                2);
    }

    /**
     * This reflects that projected columns "work" in the IonPageSource, and they do.
     * But... per the code comment in the IonPageSourceFactory, we are still materializing
     * the full base struct.
     */
    @Test
    public void testProjectedColumn()
        throws IOException
    {
        final RowType spamType = RowType.rowType(field("not_projected", BooleanType.BOOLEAN), field("eggs", INTEGER));

        HiveColumnHandle spam = new HiveColumnHandle(
                "spam",
                0,
                HiveTypeTranslator.toHiveType(spamType),
                spamType,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
        HiveColumnHandle eggs = new HiveColumnHandle(
                spam.getName(),
                spam.getBaseHiveColumnIndex(),
                spam.getBaseHiveType(),
                spam.getBaseType(),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(1),
                        ImmutableList.of("eggs"),
                        HiveType.HIVE_INT,
                        INTEGER)
                ),
                spam.getColumnType(),
                spam.getComment());

        assertRowCount(
                List.of(spam),
                List.of(eggs),
                // TODO: you can prove the optimization is effective by changing the type of `not_projected` to explode on read,
                //       we should be skipping it.
                "{ spam: { not_projected: ignore, eggs: 12 } }",
                1);
    }

    @Test
    public void testNestedProjectedColumn()
            throws IOException
    {
        final RowType barType = RowType.rowType(
                field("not_projected_2", INTEGER),
                field("qux", VARCHAR));
        final RowType fooType = RowType.rowType(
                field("baz", VARCHAR),
                field("not_projected", BooleanType.BOOLEAN),
                field("bar", barType));

        HiveColumnHandle foo = new HiveColumnHandle(
                "foo",
                0,
                HiveTypeTranslator.toHiveType(fooType),
                fooType,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
        HiveColumnHandle qux = new HiveColumnHandle(
                foo.getName(),
                foo.getBaseHiveColumnIndex(),
                foo.getBaseHiveType(),
                foo.getBaseType(),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(2, 1),
                        ImmutableList.of("bar", "qux"),
                        HiveType.HIVE_STRING,
                        VARCHAR)),
                HiveColumnHandle.ColumnType.REGULAR,
                foo.getComment());

        assertRowCount(
                List.of(foo),
                List.of(qux),
                // TODO: you can prove the optimization is effective by changing the type of `not_projected` to explode on read,
                //       we should be skipping it.
                "{ foo: { baz: bleh, not_projected: ignore, bar: { qux: quud } } }",
                1);
    }

    private void assertRowCount(List<HiveColumnHandle> columns, List<HiveColumnHandle> projectedColumns, String ionText, int rowCount)
            throws IOException
    {
        // todo: I'm sure much of this can go away once we can write Ion values and then just use TestHiveFileFormats
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of("memory:///test.ion");

        final ConnectorSession session = getHiveSession(new HiveConfig());

        int written = writeIonTextFile(ionText, location, fileSystemFactory.create(session));
        System.err.println("Wrote " + written + " bytes");

        try (ConnectorPageSource pageSource = createPageSource(fileSystemFactory, location, projectedColumns, session))
        {
            final MaterializedResult result = MaterializedResult.materializeSourceDataStream(session, pageSource, projectedColumns.stream().map(HiveColumnHandle::getType).toList());
            Assertions.assertEquals(rowCount, result.getRowCount());
        }
    }

    private int writeIonTextFile(String ionText, Location location, TrinoFileSystem fileSystem)
            throws IOException
    {
        final TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        int written = 0;
        try (final OutputStream outputStream = outputFile.create()) {
            byte[] bytes = ionText.getBytes(StandardCharsets.UTF_8);
            outputStream.write(bytes);
            outputStream.flush();
            written = bytes.length;
        }
        return written;
    }

    /**
     * todo: this is very similar to what's in TestOrcPredicates, factor out.
     */
    private static ConnectorPageSource createPageSource(
            TrinoFileSystemFactory fileSystemFactory,
            Location location,
            List<HiveColumnHandle> columns,
            ConnectorSession session)
            throws IOException
    {
        IonPageSourceFactory factory = new IonPageSourceFactory(fileSystemFactory, new HiveConfig());

        long length = fileSystemFactory.create(session).newInputFile(location).length();
        System.err.println("Found " + length + " bytes to read");

        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                "",
                ImmutableList.of(),
                columns,
                ImmutableList.of(),
                ImmutableMap.of(),
                location.toString(),
                OptionalInt.empty(),
                length,
                Instant.now().toEpochMilli());

        final Map<String, String> tableProperties = ImmutableMap.<String, String>builder()
                .put(FILE_INPUT_FORMAT, ION.getInputFormat())
                .put(SERIALIZATION_LIB, ION.getSerde())
                .put(LIST_COLUMNS, columns.stream().map(HiveColumnHandle::getName).collect(Collectors.joining(",")))
                .put(LIST_COLUMN_TYPES, columns.stream().map(HiveColumnHandle::getHiveType).map(HiveType::toString).collect(Collectors.joining(",")))
                .buildOrThrow();

        return HivePageSourceProvider.createHivePageSource(
                        ImmutableSet.of(factory),
                        session,
                        location,
                        OptionalInt.empty(),
                        0,
                        length,
                        length,
                        tableProperties,
                        TupleDomain.all(),
                        TESTING_TYPE_MANAGER,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        NO_ACID_TRANSACTION,
                        columnMappings)
                .orElseThrow();
    }
}
