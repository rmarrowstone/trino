package io.trino.hive.formats.ion;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import io.trino.spi.PageBuilder;

public interface IonDecoder
{
    /**
     * Reads the _current_ top-level-value from the IonReader.
     *
     * Expects that the calling code has called IonReader.getNext()
     * to position the reader at the value to be decoded.
     */
    void decode(IonReader reader, PageBuilder builder) throws IonException;
}
