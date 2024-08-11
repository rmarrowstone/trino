package io.trino.hive.formats.ion;

import com.amazon.ion.IonWriter;
import io.trino.spi.Page;

import java.io.IOException;

public interface IonEncoder
{
    /**
     * Encodes the Page into the IonWriter provided.
     *
     * Will flush() the writer after encoding the page.
     * Expects that the calling code is responsible for closing
     * the writer after all pages are written.
     */
    void encode(IonWriter writer, Page page) throws IOException;
}
