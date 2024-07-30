package io.trino.plugin.hive.ion;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import io.trino.hive.formats.ion.IonDecoder;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.OptionalLong;

public class IonPageSource
        implements ConnectorPageSource
{
    private final IonReader ionReader;
    private final PageBuilder pageBuilder;
    private final IonDecoder decoder;
    private int completedPositions;
    private boolean finished = false;

    public IonPageSource(IonReader ionReader, IonDecoder decoder, PageBuilder pageBuilder)
    {
        this.ionReader = ionReader;
        this.decoder = decoder;
        this.pageBuilder = pageBuilder;
        this.completedPositions = 0;
    }

    @Override
    public long getCompletedBytes()
    {
        // todo: i'm sure there's some Facet that I could get this from.
        return 1;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        while (!pageBuilder.isFull()) {
            if (!readNextValue()) {
                finished = true;
                break;
            }
        }

        Page page = pageBuilder.build();
        completedPositions += page.getPositionCount();
        pageBuilder.reset();
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return 4096;
    }

    @Override
    public void close()
            throws IOException
    {
        ionReader.close();
    }

    private boolean readNextValue()
    {
        final IonType type = ionReader.next();
        if (type == null) {
            return false;
        }

        pageBuilder.declarePosition();
        decoder.decode(ionReader, pageBuilder);
        return true;
    }
}
