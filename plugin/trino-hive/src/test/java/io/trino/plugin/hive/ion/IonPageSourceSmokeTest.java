package io.trino.plugin.hive.ion;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.TestHiveFileFormats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.RowType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.trino.plugin.hive.TestHiveFileFormats.testPageSourceFactory;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Most basic test to reflect PageSource-fu is wired up correctly.
 */
public class IonPageSourceSmokeTest
{

    @Test
    public void testReadTwoValues()
            throws IOException
    {
        assertRowCount(
                List.of(
                        new TestHiveFileFormats.TestColumn("foo", INTEGER, 31, 31),
                        new TestHiveFileFormats.TestColumn("bar", VARCHAR, "baz", "baz")),
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
        final RowType spamType = RowType.rowType(field("eggs", INTEGER), field("not_projected", VARCHAR));
        assertRowCount(
                List.of(
                        new TestHiveFileFormats.TestColumn("eggs", INTEGER, "spam", spamType, true, 12, 12, false)),
                // TODO: you can prove the optimization is effective by changing the type of `not_projected` to explode on read:
                //       we should be skipping it.
                "{ spam: { not_projected: ignore, eggs: 12 } }",
                1);
    }

    private void assertRowCount(List<TestHiveFileFormats.TestColumn> testColumns, String ionText, int rowCount)
            throws IOException
    {
        // todo: I'm sure much of this can go away once we can write Ion values and then just use TestHiveFileFormats
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        IonPageSourceFactory factory = new IonPageSourceFactory(fileSystemFactory, new HiveConfig());
        Location location = Location.of("memory:///test.ion");

        HiveSessionProperties hiveSessionProperties = new HiveSessionProperties(
                new HiveConfig(),
                new OrcReaderConfig(),
                new OrcWriterConfig()
                        .setValidationPercentage(100.0),
                new ParquetReaderConfig(),
                new ParquetWriterConfig());
        final ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(hiveSessionProperties.getSessionProperties())
                .build();

        final TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        final TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        int written = 0;
        try (final OutputStream outputStream = outputFile.create()) {
            byte[] bytes = ionText.getBytes(StandardCharsets.UTF_8);
            outputStream.write(bytes);
            outputStream.flush();
            written = bytes.length;
        }

        testPageSourceFactory(
                factory,
                location,
                HiveStorageFormat.ION,
                testColumns,
                session,
                written,
                written,
                rowCount);
    }
}
