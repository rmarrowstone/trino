package io.trino.hive.formats.ion;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import io.trino.spi.PageBuilder;

public interface IonDecoder
{
    void decode(IonReader reader, PageBuilder builder) throws IonException;
}
