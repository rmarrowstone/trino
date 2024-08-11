package io.trino.plugin.hive.ion;

import com.amazon.ion.IonReader;
import com.amazon.ion.system.IonReaderBuilder;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.ion.IonDecoder;
import io.trino.hive.formats.ion.IonDecoderFactory;
import io.trino.hive.formats.line.Column;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveClassNames.ION_SERDE_CLASS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.ReaderPageSource.noProjectionAdaptation;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.HiveUtil.splitError;

public class IonPageSourceFactory
        implements HivePageSourceFactory
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public IonPageSourceFactory(TrinoFileSystemFactory trinoFileSystemFactory, HiveConfig hiveConfig)
    {
        this.trinoFileSystemFactory = trinoFileSystemFactory;
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            Map<String, String> schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!ION_SERDE_CLASS.equals(getDeserializerClassName(schema))) {
            return Optional.empty();
        }
        checkArgument(acidInfo.isEmpty(), "Acid is not supported for Ion files");

        // Skip empty inputs
        if (length == 0) {
            return Optional.of(noProjectionAdaptation(new EmptyPageSource()));
        }

        if (start != 0) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Split start must be 0 for Ion files");
        }

        List<HiveColumnHandle> projectedReaderColumns = columns;
        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);

        if (readerProjections.isPresent()) {
            projectedReaderColumns = readerProjections.get().get().stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toImmutableList());
        }

        final TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
        final TrinoInputFile inputFile = trinoFileSystem.newInputFile(path, estimatedFileSize);

        // todo: optimization for small files that should just be read into memory
        try {
            final IonReader ionReader = IonReaderBuilder
                    .standard()
                    .build(inputFile.newStream());
            final PageBuilder pageBuilder = new PageBuilder(projectedReaderColumns.stream()
                    .map(HiveColumnHandle::getType)
                    .toList());
            final List<Column> decoderColumns = projectedReaderColumns.stream()
                    .map(hc -> new Column(hc.getName(), hc.getType(), hc.getBaseHiveColumnIndex()))
                    .toList();
            final IonDecoder decoder = IonDecoderFactory.buildDecoder(decoderColumns);
            final IonPageSource pageSource = new IonPageSource(ionReader, decoder, pageBuilder);

            return Optional.of(new ReaderPageSource(pageSource, readerProjections));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }
    }
}
