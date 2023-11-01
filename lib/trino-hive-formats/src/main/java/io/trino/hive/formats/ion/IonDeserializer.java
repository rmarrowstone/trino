package io.trino.hive.formats.ion;

import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

public class IonDeserializer implements LineDeserializer {

    @Override
    public List<? extends Type> getTypes() {
        return null;
    }

    @Override
    public void deserialize(LineBuffer lineBuffer, PageBuilder builder) throws IOException {

    }
}
