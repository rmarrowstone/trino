package io.trino.hive.formats.ion;

import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

public class IonSerializer implements LineSerializer {
    @Override
    public List<? extends Type> getTypes() {
        return null;
    }

    @Override
    public void write(Page page, int position, SliceOutput sliceOutput) throws IOException {

    }
}
