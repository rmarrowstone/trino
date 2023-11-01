package io.trino.hive.formats.ion;

import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineDeserializerFactory;

import java.util.List;
import java.util.Map;

public class IonDeserializerFactory implements LineDeserializerFactory {

    @Override
    public List<String> getHiveSerDeClassNames() {
        return IonSerDeProperties.HIVE_SERDE_CLASSNAMES;
    }

    @Override
    public LineDeserializer create(List<Column> columns, Map<String, String> serdeProperties) {
        return new IonDeserializer();
    }
}
