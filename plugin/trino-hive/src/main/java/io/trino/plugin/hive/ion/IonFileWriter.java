package io.trino.plugin.hive.ion;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.google.common.io.CountingOutputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.hive.formats.ion.IonEncoder;
import io.trino.hive.formats.ion.IonEncoderFactory;
import io.trino.hive.formats.line.Column;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.TypeManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;

public class IonFileWriter implements FileWriter
{
    private final AggregatedMemoryContext outputStreamMemoryContext;
    private final Closeable rollbackAction;
    private final IonEncoder pageEncoder;
    private final IonWriter writer;
    private final CountingOutputStream countingOutputStream;

    public IonFileWriter(
            OutputStream outputStream,
            AggregatedMemoryContext outputStreamMemoryContext,
            Closeable rollbackAction,
            TypeManager typeManager,
            List<Column> columns)
    {
        this.outputStreamMemoryContext = outputStreamMemoryContext;
        this.rollbackAction = rollbackAction;
        this.pageEncoder = IonEncoderFactory.buildEncoder(columns);
        this.countingOutputStream = new CountingOutputStream(outputStream);
        this.writer = IonTextWriterBuilder
                .minimal()
                .build(countingOutputStream);
    }

    @Override
    public long getWrittenBytes()
    {
        return countingOutputStream.getCount();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public Closeable commit()
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            try {
                rollbackAction.close();
            }
            catch (Exception _) {
                // ignore
            }
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            writer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public void appendRows(Page page)
    {
        try {
            pageEncoder.encode(writer, page);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
