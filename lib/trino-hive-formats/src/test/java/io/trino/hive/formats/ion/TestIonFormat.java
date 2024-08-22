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

import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
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
        assertValue(
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

        assertValue(
                RowType.rowType(
                        field("foo", INTEGER),
                        field("bar", VARCHAR)),
                "{ bar: null.symbol }",
                listWithNulls);
    }

    // todo: test for mistyped null and non-null values

    @Test
    public void testNestedList()
            throws IOException
    {
        assertValue(
                RowType.rowType(
                        field("primes", new ArrayType(INTEGER))),
                "{ primes: [ 17, 31, 51 ] }",
                List.of(List.of(17, 31, 51))); // todo: wrap in struct
    }

    @Test
    public void testNestedStruct()
            throws IOException
    {
        assertValue(
                RowType.rowType(
                        field("name", RowType.rowType(
                                field("first", VARCHAR),
                                field("last", VARCHAR)))),
                "{ name: { first: Woody, last: Guthrie } }",
                List.of(List.of("Woody", "Guthrie"))); // todo: wrap in struct
    }

    @Test
    public void testStructInList()
            throws IOException
    {
        assertValue(
                RowType.rowType(
                        field("elements", new ArrayType(
                                RowType.rowType(
                                        field("foo", INTEGER))))),
                "{ elements: [ { foo: 13 }, { foo: 17 } ] }",
                // yes, there are three layers of list here:
                // top-level struct (row), list of elements (array), then inner struct (row)
                List.of(
                        List.of(List.of(13), List.of(17)))); // todo: expected data
    }

    @Test
    public void testEncode()
            throws IOException
    {
        List<Column> columns = List.of(
                new Column("magic_num", INTEGER, 0),
                new Column("some_text", VARCHAR, 1),
                new Column("is_summer", BooleanType.BOOLEAN, 2),
                // todo: not working due to test util weirdness, not Ion impl
                // new Column("byte_clob", VarbinaryType.VARBINARY, 3),
                new Column("sequencer", new ArrayType(INTEGER), 4),
                new Column("struction", RowType.rowType(
                        field("foo", INTEGER),
                        field("bar", VARCHAR)), 5));

        List<?> row1 = List.of(17, "hello", true, List.of(1, 2, 3), List.of(51, "baz"));
        List<?> row2 = List.of(31, "goodbye", false, List.of(7, 8, 9), List.of(67, "qux"));

        Page page = toPage(columns, row1, row2);

        writeAndPrint(columns, page);
    }

    private void assertValue(RowType rowType, String ionText, List<Object> expected)
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
        final PageBuilder pageBuilder = new PageBuilder(1, rowType.getFields().stream().map(RowType.Field::getType).toList());

        try (IonReader ionReader = IonReaderBuilder.standard().build(ionText)) {
            assertThat(ionReader.next()).isNotNull();
            pageBuilder.declarePosition();
            decoder.decode(ionReader, pageBuilder);
            assertThat(ionReader.next()).isNull();
        }

        final List<Object> actual = readTrinoValues(columns, pageBuilder.build(), 0);
        assertColumnValuesEquals(columns, actual, expected);
    }

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
