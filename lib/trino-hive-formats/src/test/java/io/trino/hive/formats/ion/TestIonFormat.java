package io.trino.hive.formats.ion;

import com.amazon.ionhiveserde.IonHiveSerDe;
import com.amazon.ionhiveserde.objectinspectors.factories.IonObjectInspectorFactory;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.FormatTestUtils;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.hive.formats.line.openxjson.OpenXJsonOptions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.TestInstantiationException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.trino.hive.formats.FormatTestUtils.createLineBuffer;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that the trino native Ion format behaves as the legacy IonHiveSerDe.
 */
public class TestIonFormat {

    @Test
    public void testEmptyStructWithNoColumns() {
        assertRead(List.of(), "{}", List.of());
    }

    @Test
    public void testSimpleStructWithInteger() {
        assertRead(
                List.of(new Column("foo", SMALLINT, 0)),
                "{ foo: 31 }",
                List.of(31));
    }

    private void assertRead(List<Column> columns, String ionText, List<Object> expected) {
        final List<Object> hiveResult = readTextHive(columns, ionText);
        //final List<Object> trinoResult = readTextTrino(columns, ionText);

        assertEquals(expected, hiveResult);
        //assertEquals(expected, trinoResult);
    }

    private List<Object> readTextHive(List<Column> columns, String ionText) {
        final IonHiveSerDe serde = buildIonHiveSerDe(columns, new Properties());
        try {
            final Object row = serde.deserialize(new Text(ionText));

            List<Object> fieldValues = new ArrayList<>();
            StructObjectInspector rowInspector = (StructObjectInspector) serde.getObjectInspector();
            // todo: need to wire up the object inspectors to get the values as hive values and not ion
            return columns.stream()
                    .map(column -> {
                        final var fieldRef = rowInspector.getStructFieldRef(column.name());
                        rowInspector.getStructFieldData(row, fieldRef);

                        fieldRef.getFieldObjectInspector().
                    }).toList();
        } catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }

    private static IonHiveSerDe buildIonHiveSerDe(List<Column> columns, Properties props) {
        final var serde = new IonHiveSerDe();
        props.putAll(createIonSerDeColumnProperties(columns));

        try {
            serde.initialize(null, props);
            return serde;
        } catch (SerDeException e) {
            throw new TestInstantiationException("failed to initialize serde", e);
        }
    }

    private List<Object> readTextTrino(List<Column> columns, String ionText) {
        final LineDeserializer deserializer = buildIonDeserializer(columns, Map.of());
        try {
            PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
            deserializer.deserialize(createLineBuffer(ionText), pageBuilder);
            Page page = pageBuilder.build();
            return readTrinoValues(columns, page, 0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static LineDeserializer buildIonDeserializer(List<Column> columns, Map<String, String> props) {
        return new IonDeserializerFactory().create(columns, props);
    }

    private static LineSerializer buildIonSerializer(List<Column> columns, Map<String, String> props) {
        return new IonSerializerFactory().create(columns, props);
    }

    private static Map<String, String> createIonSerDeColumnProperties(List<Column> columns)
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();
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
        return schema.buildOrThrow();
    }
}
