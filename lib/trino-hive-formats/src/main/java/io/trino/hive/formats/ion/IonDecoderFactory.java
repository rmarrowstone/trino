package io.trino.hive.formats.ion;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.hive.formats.NoopType;
import io.trino.hive.formats.line.Column;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class IonDecoderFactory
{
    /**
     * Builds a decoder for the given columns.
     * <p>
     * The decoder expects to decode the _current_ Ion Value.
     * It also expects that the calling code will manage the PageBuilder.
     * <p>
     * todo: there's some logic in JsonDeserializer line 708 about resetting
     *       positions that may be an important optimization.
     */
    public static IonDecoder buildDecoder(List<Column> columns)
    {
        final RowDecoder decoder = new RowDecoder(
                columns.stream()
                        .map(c -> new RowType.Field(Optional.of(c.name()), c.type()))
                        .toList());

        return decoder::decode;
    }

    private interface BlockDecoder
    {
        void decode(IonReader reader, BlockBuilder builder);
    }

    private static BlockDecoder decoderForType(Type type)
    {
        return switch (type) {
            case IntegerType _ -> wrapDecoder(intDecoder, IonType.INT);
            case VarcharType _ -> wrapDecoder(stringDecoder, IonType.STRING, IonType.SYMBOL);
            case RowType r -> wrapDecoder(new RowDecoder(r.getFields())::decode, IonType.STRUCT);
            case ArrayType a -> wrapDecoder(new ArrayDecoder(a.getElementType()), IonType.LIST, IonType.SEXP);
            default -> throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
        };
    }

    /**
     * Wraps decoders for common handling logic.
     * <p>
     * Handles un-typed and correctly typed null values.
     * Throws for mistyped values, whether null or not.
     * Delegates to Decoder for correctly-typed, non-null values.
     * <p>
     * This code treats all values as nullable.
     */
    private static BlockDecoder wrapDecoder(BlockDecoder decoder, IonType... allowedTypes)
    {
        final Set<IonType> allowedWithNull = new HashSet<>(Arrays.asList(allowedTypes));
        allowedWithNull.add(IonType.NULL);

        return (reader, builder) -> {
            final IonType type = reader.getType();
            if (!allowedWithNull.contains(type)) {
                final String expected = allowedWithNull.stream().map(IonType::name).collect(Collectors.joining(", "));
                throw new IonException(String.format("Encountered value with IonType: %s, required one of %s ", type, expected));
            }
            if (reader.isNullValue()) {
                builder.appendNull();
            }
            else {
                decoder.decode(reader, builder);
            }
        };
    }

    /**
     * Class is both the Top-Level-Value Decoder and the Row Decoder for nested
     * structs.
     */
    private static final class RowDecoder
    {

        private final Map<String, Integer> fieldPositions;
        private final List<BlockDecoder> fieldDecoders;

        private RowDecoder(List<RowType.Field> fields)
        {
            int fieldPosition = 0;
            ImmutableList.Builder<BlockDecoder> decoderBuilder = ImmutableList.builder();
            ImmutableMap.Builder<String, Integer> fieldPositionBuilder = ImmutableMap.builder();
            for (RowType.Field field : fields) {
                if (field.getType() == NoopType.NOOP) {
                    continue;
                }
                decoderBuilder.add(decoderForType(field.getType()));
                fieldPositionBuilder.put(field.getName().get(), fieldPosition);
                fieldPosition++;
            }

            fieldPositions = fieldPositionBuilder.buildOrThrow();
            fieldDecoders = decoderBuilder.build();
        }

        private void decode(IonReader ionReader, PageBuilder pageBuilder)
        {
            // todo: also map lists?
            if (ionReader.getType() != IonType.STRUCT) {
                throw new IonException("Top Level Values must be Structs!");
            }
            if (ionReader.isNullValue()) {
                // todo: is this an error? it is in spark.
                //       what does ion-java-path-extraction do here?
                //       OpenXJson returns top-level nulls
                throw new IonException("Top Level Values must not be null!");
            }
            decode(ionReader, pageBuilder::getBlockBuilder);
        }

        private void decode(IonReader ionReader, BlockBuilder blockBuilder)
        {
            ((RowBlockBuilder) blockBuilder)
                    .buildEntry(fieldBuilders -> decode(ionReader, fieldBuilders::get));
        }

        // assumes that the reader is positioned on a non-null struct value
        private void decode(IonReader ionReader, IntFunction<BlockBuilder> blockSelector)
        {
            boolean[] encountered = new boolean[fieldDecoders.size()];
            ionReader.stepIn();

            // todo: consider optimization where we step out after encountering all mapped fields.
            //       but that requires that we only take the first instance of any duplicate field
            //       and ignore subsequent values. otherwise it would be surprising for users.
            while (ionReader.next() != null) {
                // todo: case insensitivity
                final Integer fieldIndex = fieldPositions.get(ionReader.getFieldName());
                if (fieldIndex == null) {
                    continue;
                }
                if (encountered[fieldIndex]) {
                    // todo: ignore, throw, or overwrite?
                    System.out.println("Duplicate field encountered. Is this an error?");
                }
                encountered[fieldIndex] = true;
                final BlockBuilder blockBuilder = blockSelector.apply(fieldIndex);
                fieldDecoders.get(fieldIndex).decode(ionReader, blockBuilder);
            }

            for (int i = 0; i < encountered.length; i++) {
                if (!encountered[i]) {
                    blockSelector.apply(i).appendNull();
                }
            }

            ionReader.stepOut();
        }
    }

    private static final class ArrayDecoder
            implements BlockDecoder
    {
        private final BlockDecoder elementDecoder;

        private ArrayDecoder(Type elementType)
        {
            this.elementDecoder = decoderForType(elementType);
        }

        @Override
        public void decode(IonReader ionReader, BlockBuilder blockBuilder)
        {
            ((ArrayBlockBuilder) blockBuilder)
                    .buildEntry(elementBuilder -> {
                        ionReader.stepIn();
                        while (ionReader.next() != null) {
                            elementDecoder.decode(ionReader, elementBuilder);
                        }
                        ionReader.stepOut();
                    });
        }
    }

    private static final BlockDecoder intDecoder = (ionReader, blockBuilder) ->
            INTEGER.writeLong(blockBuilder, ionReader.longValue());

    private static final BlockDecoder stringDecoder = (ionReader, blockBuilder) -> {
        final String value = ionReader.stringValue();
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(value));
    };
}
