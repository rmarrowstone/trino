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

import com.amazon.ion.IonBool;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ionhiveserde.IonHiveSerDe;
import com.amazon.ionhiveserde.objectinspectors.IonFloatToDoubleObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonNumberToDecimalObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTextToVarcharObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonValueToStringObjectInspector;
import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.FormatTestUtils;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static io.trino.hive.formats.FormatTestUtils.assertColumnValuesEquals;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.FormatTestUtils.toPage;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.math.RoundingMode.HALF_UP;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIonFormat
{
    private static final String FAIL_ON_OVERFLOW = "ion.fail_on_overflow";

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
    public void testCaseInsensitivityOfKeys()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("Foo", INTEGER),
                        field("BAR", VARCHAR)),
                "{ Bar: baz, foo: 31 }",
                List.of(31, "baz"));
    }

    @Test
    public void testCaseInsensitivityOfDuplicateKeys()
            throws IOException
    {
        // this test asserts that duplicate key behavior works as expected(i.e. capturing the last value),
        // for duplicate keys with different casing.
        assertValues(
                RowType.rowType(
                        field("Foo", INTEGER),
                        field("BAR", VARCHAR)),
                "{ bar: baz, Foo: 31, foo: 5 }",
                List.of(5, "baz"));
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

        List<Object> row1 = List.of(17, "something", true, new SqlVarbinary(new byte[] {(byte) 0xff}), List.of(1, 2, 3), List.of(51, "baz"));
        List<Object> row2 = List.of(31, "somebody", false, new SqlVarbinary(new byte[] {(byte) 0x01, (byte) 0xaa}), List.of(7, 8, 9), List.of(67, "qux"));
        String ionText = """
                { magic_num:17, some_text:"something", is_summer:true, byte_clob:{{/w==}}, sequencer:[1,2,3], struction:{ foo:51, bar:"baz"}}
                { magic_num:31, some_text:"somebody", is_summer:false, byte_clob:{{Aao=}}, sequencer:[7,8,9], struction:{ foo:67, bar:"qux"}}
                """;

        Page page = toPage(columns, row1, row2);
        assertIonEquivalence(columns, page, ionText);
    }

    private void assertValues(RowType rowType, String ionText, List<Object>... expected)
            throws IOException
    {
        List<RowType.Field> fields = rowType.getFields();
        List<Column> columns = IntStream.range(0, fields.size())
                .boxed()
                .map(i -> {
                    final RowType.Field field = fields.get(i);
                    return new Column(field.getName().get(), field.getType(), i);
                })
                .toList();
        IonDecoder decoder = IonDecoderFactory.buildDecoder(columns);
        PageBuilder pageBuilder = new PageBuilder(expected.length, rowType.getFields().stream().map(RowType.Field::getType).toList());

        try (IonReader ionReader = IonReaderBuilder.standard().build(ionText)) {
            for (int i = 0; i < expected.length; i++) {
                assertThat(ionReader.next()).isNotNull();
                pageBuilder.declarePosition();
                decoder.decode(ionReader, pageBuilder);
            }
            assertThat(ionReader.next()).isNull();
        }

        for (int i = 0; i < expected.length; i++) {
            List<Object> actual = readTrinoValues(columns, pageBuilder.build(), i);
            assertColumnValuesEquals(columns, actual, expected[i]);
        }
    }

    private static Deserializer createHiveDeserializer(List<Column> columns, boolean failOnOverflow)
            throws SerDeException
    {
        Configuration configuration = new Configuration(false);
        Properties schema = new Properties();
        schema.put(LIST_COLUMNS, columns.stream()
                .sorted(Comparator.comparing(Column::ordinal))
                .map(Column::name)
                .collect(joining(",")));
        schema.put(LIST_COLUMN_TYPES, columns.stream()
                .sorted(Comparator.comparing(Column::ordinal))
                .map(Column::type)
                .map(FormatTestUtils::getJavaObjectInspector)
                .map(ObjectInspector::getTypeName)
                .collect(joining(",")));
        schema.put(FAIL_ON_OVERFLOW, Boolean.toString(failOnOverflow));
        Deserializer deserializer = new IonHiveSerDe();
        deserializer.initialize(configuration, schema);
        configuration.set(SERIALIZATION_LIB, deserializer.getClass().getName());
        return deserializer;
    }

    private static void assertValue(Type type, String ionValue, Object expected) throws SerDeException {
        assertValueHive(type, ionValue, expected,false);
        assertValueTrino(type, ionValue, expected);
    }

    private static void assertValueTrino(Type type, String ionValue, Object expected)
    {
        String ionText = "{test : " + ionValue + "}";
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        IonDecoder decoder = IonDecoderFactory.buildDecoder(List.of(new Column("test", type, 33)));
        IonReader ionReader = IonReaderBuilder.standard().build(ionText);
        PageBuilder pageBuilder = new PageBuilder(1,
                List.of(type));
        assertThat(ionReader.next()).isNotNull();
        pageBuilder.declarePosition();
        decoder.decode(ionReader, pageBuilder);
        Assertions.assertEquals(columns.getFirst().type().getObjectValue(null, pageBuilder.build().getBlock(0), 0), expected);
    }

    private static void assertValueHive(Type type, String ionValue, Object expected, boolean failOnOverflow) throws SerDeException {
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        String ionText = "{test: " + ionValue + "}";
        Deserializer deserializer = createHiveDeserializer(columns, failOnOverflow);
        Object rowData = deserializer.deserialize(new Text(ionText));

        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        for (Column column : columns) {
            StructField field = rowInspector.getStructFieldRef(column.name());
            Object fieldValue = rowInspector.getStructFieldData(rowData, field);
            ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();
            switch (column.type()) {
                case VarcharType _ -> {
                    String result =
                            ((IonValueToStringObjectInspector) fieldObjectInspector).getPrimitiveJavaObject(fieldValue);
                    Assertions.assertEquals(result, expected);
                    return;
                }
                case DecimalType _ -> {
                    if (fieldValue instanceof HiveDecimal decimal) {
                        DecimalType decimalType = (DecimalType) type;
                        // writable messes with the scale so rescale the values to the Trino type
                        BigDecimal bigDecimal = decimal.bigDecimalValue();
                        bigDecimal = bigDecimal.setScale(decimalType.getScale(), HALF_UP);
                        if (bigDecimal.precision() > decimalType.getPrecision()) {
                            throw new IllegalArgumentException("decimal precision larger than column precision");
                        }
                        Assertions.assertEquals(bigDecimal, expected);
                        return;
                    } else if (fieldValue instanceof IonDecimal decimal) {
                        HiveDecimal hiveDecimal =
                                ((IonNumberToDecimalObjectInspector) fieldObjectInspector).getPrimitiveJavaObject(decimal);
                        DecimalType decimalType = (DecimalType) type;
                        // writable messes with the scale so rescale the values to the Trino type
                        BigDecimal bigDecimal = hiveDecimal.bigDecimalValue();
                        bigDecimal = bigDecimal.setScale(decimalType.getScale(), HALF_UP);
                        if (bigDecimal.precision() > decimalType.getPrecision()) {
                            throw new IllegalArgumentException("decimal precision larger than column precision");
                        }
                        Assertions.assertEquals(bigDecimal, expected);
                        return;
                    }
                }
                case TinyintType _ -> {
                    Object result =
                            ((IonIntToTinyIntObjectInspector) fieldObjectInspector).getPrimitiveJavaObject(fieldValue);
                    Assertions.assertEquals(result, expected);
                }
                //TODO: Modify this to add more cases when adding more tests for a trino type
                default -> throw new IllegalStateException("Unexpected value: " + column.type());
            }

        }
    }

    private static void assertValueFails(Type type, String ionValue) {
        assertValueFailsHive(type, ionValue, false);
        assertValueFailsTrino(type, ionValue);
    }

    private static void assertValueFails(Type type, String ionValue, boolean failOnOverflow)
    {
        assertValueFailsHive(type, ionValue, failOnOverflow);
        assertValueFailsTrino(type, ionValue);
    }

    private static void assertValueFailsTrino(Type type, String ionValue)
    {
        String ionText = "{test : " + ionValue + "}";
        IonDecoder decoder = IonDecoderFactory.buildDecoder(List.of(new Column("test", type, 33)));
        assertThatThrownBy(() -> {
            IonReader ionReader = IonReaderBuilder.standard().build(ionText);
            PageBuilder pageBuilder = new PageBuilder(1,
                    List.of(type));
            assertThat(ionReader.next()).isNotNull();
            pageBuilder.declarePosition();
            decoder.decode(ionReader, pageBuilder);
        }).isInstanceOf(Exception.class);
    }

    private static void assertValueFailsHive(Type type, String ionValue, boolean failOnOverflow)
    {
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        String ionText = "{test: " + ionValue + "}";
        assertThatThrownBy(() -> {
            Deserializer deserializer = createHiveDeserializer(columns, failOnOverflow);
            Object rowData = deserializer.deserialize(new Text(ionText));

            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
            for (Column column : columns) {
                StructField field = rowInspector.getStructFieldRef(column.name());
                Object fieldValue = rowInspector.getStructFieldData(rowData, field);
                ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();
                 switch (column.type()) {
                     case BooleanType _ -> {
                         if (fieldValue instanceof BooleanWritable booleanWritable) {
                             booleanWritable.get();
                             return;
                         } else if (fieldValue instanceof IonBool booleanValue) {
                             booleanValue.booleanValue();
                             return;
                         }
                     }
                     case VarcharType _ -> {
                         if (fieldValue instanceof HiveVarchar varchar) {
                             varchar.getValue();
                             return;
                         } else if (fieldValue instanceof IonString string) {
                             string.stringValue();
                             return;
                         } else if (fieldValue instanceof IonSymbol symbol) {
                             ((IonTextToVarcharObjectInspector) fieldObjectInspector).getPrimitiveJavaObject(symbol);
                             symbol.stringValue();
                             return;
                         }
                     }
                     case CharType _ -> {
                         if (fieldValue instanceof HiveChar chars) {
                             chars.getValue();
                             return;
                         } else if (fieldValue instanceof IonString string) {
                             string.stringValue();
                             return;
                         } else if (fieldValue instanceof IonSymbol symbol) {
                             symbol.stringValue();
                             return;
                         }
                     }
                     case DoubleType _ -> {
                         if (fieldValue instanceof DoubleWritable doubleWritable) {
                             doubleWritable.get();
                             return;
                         } else if (fieldValue instanceof IonFloat floatValue) {
                             ((IonFloatToDoubleObjectInspector)fieldObjectInspector).getPrimitiveJavaObject(floatValue);
                             floatValue.doubleValue();
                             return;
                         }
                     }
                     case DecimalType _ -> {
                         if (fieldValue instanceof HiveDecimal decimal) {
                             DecimalType decimalType = (DecimalType) type;
                             // writable messes with the scale so rescale the values to the Trino type
                             BigDecimal bigDecimal = decimal.bigDecimalValue();
                             bigDecimal = bigDecimal.setScale(decimalType.getScale(), HALF_UP);
                             if (bigDecimal.precision() > decimalType.getPrecision()) {
                                 throw new IllegalArgumentException("decimal precision larger than column precision");
                             }
                             return;
                         } else if (fieldValue instanceof IonDecimal decimal) {
                             HiveDecimal hiveDecimal =
                                     ((IonNumberToDecimalObjectInspector) fieldObjectInspector).getPrimitiveJavaObject(decimal);
                             DecimalType decimalType = (DecimalType) type;
                             // writable messes with the scale so rescale the values to the Trino type
                             BigDecimal bigDecimal = hiveDecimal.bigDecimalValue();
                             bigDecimal = bigDecimal.setScale(decimalType.getScale(), HALF_UP);
                             if (bigDecimal.precision() > decimalType.getPrecision()) {
                                 throw new IllegalArgumentException("decimal precision larger than column precision");
                             }
                             return;
                         }
                     }
                     //TODO: Modify this to add more cases when adding more tests for a trino type
                     default -> throw new IllegalStateException("Unexpected value: " + column.type());
                 }
                 throw new IllegalStateException("Unexpected value: " + column.type());
            }
        });
    }

    /**
     * Encodes the page as Ion and asserts its equivalence to ionText, per the Ion datamodel.
     * <br>
     * This allows us to make assertions about how the data is encoded that may be equivalent
     * in the trino datamodel but distinct per the Ion datamodel. Some examples:
     * - absent fields vs null field values
     * - Symbol vs String for text values
     * - Timestamps with UTC vs unknown offset
     */
    private void assertIonEquivalence(List<Column> columns, Page page, String ionText)
            throws IOException
    {
        IonSystem system = IonSystemBuilder.standard().build();
        IonDatagram datagram = system.newDatagram();
        IonEncoder encoder = IonEncoderFactory.buildEncoder(columns);
        IonWriter ionWriter = system.newWriter(datagram);
        encoder.encode(ionWriter, page);
        ionWriter.close();

        IonDatagram expected = system.getLoader().load(ionText);
        Assertions.assertEquals(datagram.size(), expected.size());
        for (int i = 0; i < expected.size(); i++) {
            // IonValue.equals() is Ion model equivalence.
            Assertions.assertEquals(expected.get(i), datagram.get(i));
        }
    }
}
