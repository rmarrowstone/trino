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
package io.trino.hive.formats.ion;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarbinaryType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.trino.hive.formats.FormatTestUtils.assertColumnValuesEquals;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.FormatTestUtils.toPage;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIonFormat
{
    @Test
    public void testSuperBasicStruct()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("foo", INTEGER),
                        field("bar", VARCHAR)),
                "{ bar: baz, foo: 31, ignored: true }",
                List.of(31, "baz"));
    }

    @Test
    public void testStructWithNullAndMissingValues()
            throws IOException
    {
        final List<Object> listWithNulls = new ArrayList<>();
        listWithNulls.add(null);
        listWithNulls.add(null);

        assertValues(
                RowType.rowType(
                        field("foo", INTEGER),
                        field("bar", VARCHAR)),
                "{ bar: null.symbol }",
                listWithNulls);
    }

    @Test
    public void testStructWithDuplicateKeys()
            throws IOException
    {
        // this test is not making a value judgement; capturing the last
        // is not necessarily the "right" behavior. the test just
        // documents what the behavior is, which is based on the behavior
        // of the hive serde, and is consistent with the trino json parser.
        assertValues(
                RowType.rowType(field("foo", INTEGER)),
                "{ foo: 17, foo: 31, foo: 53 } { foo: 67 }",
                List.of(53), List.of(67));
    }

    // todo: test for mistyped null and non-null values

    @Test
    public void testNestedList()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("primes", new ArrayType(INTEGER))),
                "{ primes: [ 17, 31, 51 ] }",
                List.of(List.of(17, 31, 51)));
    }

    @Test
    public void testNestedStruct()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("name", RowType.rowType(
                                field("first", VARCHAR),
                                field("last", VARCHAR)))),
                "{ name: { first: Woody, last: Guthrie } }",
                List.of(List.of("Woody", "Guthrie")));
    }

    @Test
    public void testStructInList()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("elements", new ArrayType(
                                RowType.rowType(
                                        field("foo", INTEGER))))),
                "{ elements: [ { foo: 13 }, { foo: 17 } ] }",
                // yes, there are three layers of list here:
                // top-level struct (row), list of elements (array), then inner struct (row)
                List.of(
                        List.of(List.of(13), List.of(17))));
    }

    @Test
    public void testPicoPreciseTimestamp()
            throws IOException
    {
        Timestamp ionTimestamp = Timestamp.forSecond(2067, 8, 9, 11, 22, new BigDecimal("33.445566"), 0);
        long epochMicros = ionTimestamp.getDecimalMillis().movePointRight(3).longValue();
        assertValues(
                RowType.rowType(field("my_ts", TimestampType.TIMESTAMP_PICOS)),
                "{ my_ts: 2067-08-09T11:22:33.445566778899Z }",
                List.of(SqlTimestamp.newInstance(12, epochMicros, 778899)));
    }

    @Test
    public void testOverPreciseTimestamps()
            throws IonException
    {
        // todo: implement
    }

    @Test
    public void testDecimalPrecisionAndScale()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("amount", DecimalType.createDecimalType(10, 2)),
                        field("big_amount", DecimalType.createDecimalType(25, 5))),
                "{ amount: 1234.00, big_amount: 1234.00000 }"
                        + "{ amount: 1234d0, big_amount: 1234d0 }"
                        + "{ amount: 12d2, big_amount: 12d2 }"
                        + "{ amount: 1234.000, big_amount: 1234.000000 }",
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(120000), 10, 2), new SqlDecimal(BigInteger.valueOf(120000000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)));
    }

    @Test
    public void testOversizeOrOverpreciseDecimals()
    {
        // todo: implement
    }

    @Test
    public void testEncode()
            throws IOException
    {
        List<Column> columns = List.of(
                new Column("magic_num", INTEGER, 0),
                new Column("some_text", VARCHAR, 1),
                new Column("is_summer", BooleanType.BOOLEAN, 2),
                new Column("byte_clob", VarbinaryType.VARBINARY, 3),
                new Column("sequencer", new ArrayType(INTEGER), 4),
                new Column("struction", RowType.rowType(
                        field("foo", INTEGER),
                        field("bar", VARCHAR)), 5));

        List<?> row1 = List.of(17, "hello", true, new SqlVarbinary(new byte[] {(byte) 0xff}), List.of(1, 2, 3), List.of(51, "baz"));
        List<?> row2 = List.of(31, "goodbye", false, new SqlVarbinary(new byte[] {(byte) 0x01, (byte) 0xaa}), List.of(7, 8, 9), List.of(67, "qux"));

        Page page = toPage(columns, row1, row2);

        writeAndPrint(columns, page);
    }

    private void assertValues(RowType rowType, String ionText, List<Object>... expected)
            throws IOException
    {
        final List<RowType.Field> fields = rowType.getFields();
        final List<Column> columns = IntStream.range(0, fields.size())
                .boxed()
                .map(i -> {
                    final RowType.Field field = fields.get(i);
                    return new Column(field.getName().get(), field.getType(), i);
                })
                .toList();
        final IonDecoder decoder = IonDecoderFactory.buildDecoder(columns);
        final PageBuilder pageBuilder = new PageBuilder(expected.length, rowType.getFields().stream().map(RowType.Field::getType).toList());

        try (IonReader ionReader = IonReaderBuilder.standard().build(ionText)) {
            for (int i = 0; i < expected.length; i++) {
                assertThat(ionReader.next()).isNotNull();
                pageBuilder.declarePosition();
                decoder.decode(ionReader, pageBuilder);
            }
            assertThat(ionReader.next()).isNull();
        }

        for (int i = 0; i < expected.length; i++) {
            final List<Object> actual = readTrinoValues(columns, pageBuilder.build(), i);
            assertColumnValuesEquals(columns, actual, expected[i]);
        }
    }

    // todo: this needs to assert, not print to stderr!
    private void writeAndPrint(List<Column> columns, Page page)
            throws IOException
    {
        final IonEncoder encoder = IonEncoderFactory.buildEncoder(columns);
        final IonWriter ionWriter = IonTextWriterBuilder.standard()
                .build((OutputStream) System.err);
        encoder.encode(ionWriter, page);
        ionWriter.close();
    }
}
