package io.trino.hive.formats.ion;

import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.hive.formats.line.LineSerializerFactory;

import java.util.List;
import java.util.Map;

public class IonSerializerFactory implements LineSerializerFactory {
    @Override
    public List<String> getHiveSerDeClassNames() {
        return IonSerDeProperties.HIVE_SERDE_CLASSNAMES;
    }

    @Override
    public LineSerializer create(List<Column> columns, Map<String, String> serdeProperties) {
        return new IonSerializer();
    }
}
