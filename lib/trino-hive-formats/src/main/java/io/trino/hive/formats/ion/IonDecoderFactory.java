/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.hive.formats.ion;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public class IonDecoderFactory
{
    private IonDecoderFactory() {}

    /**
     * Builds a decoder for the given columns.
     * <br>
     * The decoder expects to decode the _current_ Ion Value, meaning that the caller
     * is responsible for iterating over the Ion value stream by calling next() until
     * the stream is exhausted.
     * <br>
     * It also expects that the calling code will manage the PageBuilder.
     * <br>
     * If there are no columns to decode (e.g. for a `count(*)`), then this returns
     * a no-op decoder. There is no type-checking for this case: any Ion value of
     * Ion type, including null, is just ignored. This is the behavior for the legacy
     * ion hive serde.
     */
    public static IonDecoder buildDecoder(List<DecoderColumn> columns)
    {
        if (columns.isEmpty()) {
            return (_, _) -> {};
        }

        DecoderInstructions instructions = DecoderInstructions.forColumns(columns);
        IndirectDecoder decoder = decoderForInstructions(instructions);
        return (ionReader, pageBuilder) -> {
            decoder.decode(ionReader, pageBuilder::getBlockBuilder);
        };
    }

    private static IndirectDecoder decoderForInstructions(DecoderInstructions instructions)
    {
        return switch (instructions) {
            case DecoderInstructions.StructFields structFields -> {
                Map<String, IndirectDecoder> decoders = structFields.fields().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> decoderForInstructions(e.getValue())));
                yield new RowDecoder(decoders);
            }
            case DecoderInstructions.DecodeValue decodeColumn -> indirectDecoderFor(
                    decoderForType(decodeColumn.type()),
                    decodeColumn.blockPosition());
            case DecoderInstructions.IndexedItems indexedItems -> {
                Map<Integer, IndirectDecoder> decoders = indexedItems.items().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> decoderForInstructions(e.getValue())));
                yield new IndexedItemDecoder(decoders);
            }
            case DecoderInstructions.Empty _ -> throw new IllegalArgumentException("Cannot build Decoder for Empty Instructions!");
        };
    }

    /**
     * This interface abstracts any actual block builder manipulation to allow for 'pass-through'
     * or 'dereference' decoders that don't manipulate BlockBuilders themselves but have children
     * or other ancestors that do.
     */
    private interface IndirectDecoder
    {
        /**
         * Decode the value and any descendents from the current ionReader position.
         */
        void decode(IonReader ionReader, IntFunction<BlockBuilder> builderSelector);

        /**
         * Denote an expected value as absent or missing to the descendents.
         * Practically this is treated as a null.
         */
        void absent(IntFunction<BlockBuilder> builderSelector);

        /**
         * Reset any descendent blocks to the previous position.
         * Used when a duplicate key is encountered in a Struct.
         */
        void reset(IntFunction<BlockBuilder> builderSelector);
    }

    /**
     * Closes over the position for selecting a BlockBuilder from the parent builder.
     * <br>
     * Handles reset and absent events from a non-materialized parent.
     */
    private static IndirectDecoder indirectDecoderFor(BlockDecoder decoder, int position)
    {
        return new IndirectDecoder()
        {
            @Override
            public void decode(IonReader ionReader, IntFunction<BlockBuilder> builderSelector)
                    throws IonException
            {
                decoder.decode(ionReader, builderSelector.apply(position));
            }

            @Override
            public void reset(IntFunction<BlockBuilder> builderSelector)
            {
                BlockBuilder builder = builderSelector.apply(position);
                builder.resetTo(builder.getPositionCount() - 1);
            }

            @Override
            public void absent(IntFunction<BlockBuilder> builderSelector)
            {
                builderSelector.apply(position).appendNull();
            }
        };
    }

    private static class IndexedItemDecoder
            implements IndirectDecoder
    {
        private List<IndirectDecoder> itemDecoders;

        public IndexedItemDecoder(Map<Integer, IndirectDecoder> items)
        {
            int maxIndex = items.keySet().stream().reduce(-1, Math::max);
            IndirectDecoder[] decoders = new IndirectDecoder[maxIndex + 1];

            for (Map.Entry<Integer, IndirectDecoder> item : items.entrySet()) {
                decoders[item.getKey()] = item.getValue();
            }
            this.itemDecoders = Arrays.asList(decoders);
        }

        @Override
        public void decode(IonReader ionReader, IntFunction<BlockBuilder> builderSelector)
        {
            if (ionReader.isNullValue()) {
                absent(builderSelector);
            }
            else {
                if (ionReader.getType() != IonType.LIST && ionReader.getType() != IonType.SEXP) {
                    throw new IonException("Expected sequence type for indexed item! Encountered: " + ionReader.getType());
                }
                int position = 0;
                ionReader.stepIn();
                while (ionReader.next() != null && position < itemDecoders.size()) {
                    IndirectDecoder itemDecoder = itemDecoders.get(position);
                    if (itemDecoder != null) {
                        itemDecoder.decode(ionReader, builderSelector);
                    }
                    position++;
                }
                ionReader.stepOut();
            }
        }

        @Override
        public void absent(IntFunction<BlockBuilder> builderSelector)
        {
            for (IndirectDecoder decoder : itemDecoders) {
                decoder.absent(builderSelector);
            }
        }

        @Override
        public void reset(IntFunction<BlockBuilder> builderSelector)
        {
            for (IndirectDecoder decoder : itemDecoders) {
                decoder.reset(builderSelector);
            }
        }
    }

    /**
     * BlockDecoders work directly on the BlockBuilders, actually decoding Ion
     * data and putting it into blocks.
     */
    private interface BlockDecoder
    {
        void decode(IonReader reader, BlockBuilder builder);
    }

    private static BlockDecoder decoderForType(Type type)
    {
        return switch (type) {
            case TinyintType _ -> wrapDecoder(byteDecoder, IonType.INT);
            case SmallintType _ -> wrapDecoder(shortDecoder, IonType.INT);
            case IntegerType _ -> wrapDecoder(intDecoder, IonType.INT);
            case BigintType _ -> wrapDecoder(longDecoder, IonType.INT);
            case RealType _ -> wrapDecoder(realDecoder, IonType.FLOAT);
            case DoubleType _ -> wrapDecoder(floatDecoder, IonType.FLOAT);
            case BooleanType _ -> wrapDecoder(boolDecoder, IonType.BOOL);
            case DateType _ -> wrapDecoder(dateDecoder, IonType.TIMESTAMP);
            case TimestampType t -> wrapDecoder(timestampDecoder(t), IonType.TIMESTAMP);
            case DecimalType t -> wrapDecoder(decimalDecoder(t), IonType.DECIMAL);
            case VarcharType _, CharType _ -> wrapDecoder(stringDecoder, IonType.STRING, IonType.SYMBOL);
            case VarbinaryType _ -> wrapDecoder(binaryDecoder, IonType.BLOB, IonType.CLOB);
            case RowType r -> wrapDecoder(new RowDecoder(r.getFields()), IonType.STRUCT);
            case ArrayType a -> wrapDecoder(new ArrayDecoder(decoderForType(a.getElementType())), IonType.LIST, IonType.SEXP);
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
     * This class acts as both the "indirect" decoder for top-level structs and dereference paths
     * and as the "direct" BlockDecoder for nested RowTypes.
     * When used for a nested RowType (within another materialized RowType), it is expected to be
     * wrapped with the `indirectDecoderFor`, as that will work on the builder for the row itself,
     * which is what you want for that usage.
     */
    private static class RowDecoder
            implements BlockDecoder, IndirectDecoder
    {
        private final Map<String, Integer> fieldPositions;
        private final List<IndirectDecoder> fieldDecoders;

        RowDecoder(List<RowType.Field> fields)
        {
            ImmutableMap.Builder<String, Integer> positionBuilder = ImmutableMap.builder();
            ImmutableList.Builder<IndirectDecoder> decoderBuilder = ImmutableList.builder();
            int position = 0;
            for (RowType.Field field : fields) {
                decoderBuilder.add(indirectDecoderFor(decoderForType(field.getType()), position));
                positionBuilder.put(field.getName().get().toLowerCase(Locale.ROOT), position);
                position++;
            }

            fieldPositions = positionBuilder.buildOrThrow();
            fieldDecoders = decoderBuilder.build();
        }

        RowDecoder(Map<String, IndirectDecoder> decoderMap)
        {
            ImmutableMap.Builder<String, Integer> positionBuilder = ImmutableMap.builder();
            ImmutableList.Builder<IndirectDecoder> decoderBuilder = ImmutableList.builder();
            int position = 0;
            for (Map.Entry<String, IndirectDecoder> field : decoderMap.entrySet()) {
                decoderBuilder.add(field.getValue());
                positionBuilder.put(field.getKey().toLowerCase(Locale.ROOT), position++);
            }
            fieldPositions = positionBuilder.buildOrThrow();
            fieldDecoders = decoderBuilder.build();
        }

        @Override
        public void decode(IonReader ionReader, BlockBuilder blockBuilder)
        {
            ((RowBlockBuilder) blockBuilder)
                    .buildEntry(fieldBuilders -> decodeChildren(ionReader, fieldBuilders::get));
        }

        @Override
        public void decode(IonReader ionReader, IntFunction<BlockBuilder> builderSelector)
        {
            if (ionReader.isNullValue()) {
                absent(builderSelector);
            }
            else {
                if (ionReader.getType() != IonType.STRUCT) {
                    throw new IonException("RowType must be Structs! Encountered: " + ionReader.getType());
                }
                decodeChildren(ionReader, builderSelector);
            }
        }

        private void decodeChildren(IonReader ionReader, IntFunction<BlockBuilder> builderSelector)
        {
            // assumes that the reader is positioned on a non-null struct value
            boolean[] encountered = new boolean[fieldDecoders.size()];
            ionReader.stepIn();

            while (ionReader.next() != null) {
                final Integer fieldIndex = fieldPositions.get(ionReader.getFieldName().toLowerCase(Locale.ROOT));
                if (fieldIndex == null) {
                    continue;
                }
                IndirectDecoder decoder = fieldDecoders.get(fieldIndex);
                if (encountered[fieldIndex]) {
                    decoder.reset(builderSelector);
                }
                else {
                    encountered[fieldIndex] = true;
                }
                decoder.decode(ionReader, builderSelector);
            }

            for (int i = 0; i < encountered.length; i++) {
                if (!encountered[i]) {
                    fieldDecoders.get(i).absent(builderSelector);
                }
            }

            ionReader.stepOut();
        }

        @Override
        public void reset(IntFunction<BlockBuilder> builderSelector)
        {
            for (IndirectDecoder fieldDecoder : fieldDecoders) {
                fieldDecoder.reset(builderSelector);
            }
        }

        @Override
        public void absent(IntFunction<BlockBuilder> builderSelector)
        {
            for (IndirectDecoder fieldDecoder : fieldDecoders) {
                fieldDecoder.absent(builderSelector);
            }
        }
    }

    private record ArrayDecoder(BlockDecoder elementDecoder)
            implements BlockDecoder
    {
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

    private static BlockDecoder timestampDecoder(TimestampType type)
    {
        // todo: no attempt is made at handling offsets or lack thereof
        if (type.isShort()) {
            return (reader, builder) -> {
                long micros = reader.timestampValue().getDecimalMillis()
                        .setScale(type.getPrecision() - 3, RoundingMode.HALF_EVEN)
                        .movePointRight(3)
                        .longValue();
                type.writeLong(builder, micros);
            };
        }
        else {
            return (reader, builder) -> {
                BigDecimal decimalMicros = reader.timestampValue().getDecimalMillis()
                        .movePointRight(3);
                BigDecimal subMicrosFrac = decimalMicros.remainder(BigDecimal.ONE)
                        .movePointRight(6);
                type.writeObject(builder, new LongTimestamp(decimalMicros.longValue(), subMicrosFrac.intValue()));
            };
        }
    }

    private static BlockDecoder decimalDecoder(DecimalType type)
    {
        if (type.isShort()) {
            return (reader, builder) -> {
                long unscaled = reader.bigDecimalValue()
                        .setScale(type.getScale(), RoundingMode.UNNECESSARY)
                        .unscaledValue()
                        .longValue();
                type.writeLong(builder, unscaled);
            };
        }
        else {
            return (reader, builder) -> {
                Int128 unscaled = Int128.valueOf(reader.bigDecimalValue()
                        .setScale(type.getScale(), RoundingMode.UNNECESSARY)
                        .unscaledValue());
                type.writeObject(builder, unscaled);
            };
        }
    }

    private static final BlockDecoder byteDecoder = (ionReader, blockBuilder) ->
            TinyintType.TINYINT.writeLong(blockBuilder, ionReader.longValue());

    private static final BlockDecoder shortDecoder = (ionReader, blockBuilder) ->
            SmallintType.SMALLINT.writeLong(blockBuilder, ionReader.longValue());

    private static final BlockDecoder intDecoder = (ionReader, blockBuilder) ->
            IntegerType.INTEGER.writeLong(blockBuilder, ionReader.longValue());

    private static final BlockDecoder longDecoder = (ionReader, blockBuilder) ->
            BigintType.BIGINT.writeLong(blockBuilder, ionReader.longValue());

    private static final BlockDecoder realDecoder = (ionReader, blockBuilder) -> {
        double readValue = ionReader.doubleValue();
        if (readValue == (float) readValue) {
            RealType.REAL.writeFloat(blockBuilder, (float) ionReader.doubleValue());
        }
        else {
            // todo: some kind of "permissive truncate" flag
            throw new IllegalArgumentException("Won't truncate double precise float to real!");
        }
    };

    private static final BlockDecoder floatDecoder = (ionReader, blockBuilder) ->
            DoubleType.DOUBLE.writeDouble(blockBuilder, ionReader.doubleValue());

    private static final BlockDecoder stringDecoder = (ionReader, blockBuilder) ->
            VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(ionReader.stringValue()));

    private static final BlockDecoder boolDecoder = (ionReader, blockBuilder) ->
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, ionReader.booleanValue());

    private static final BlockDecoder dateDecoder = (ionReader, blockBuilder) ->
            DateType.DATE.writeLong(blockBuilder, ionReader.timestampValue().dateValue().toInstant().atZone(ZoneId.of("UTC")).toLocalDate().toEpochDay());

    private static final BlockDecoder binaryDecoder = (ionReader, blockBuilder) ->
            VarbinaryType.VARBINARY.writeSlice(blockBuilder, Slices.wrappedBuffer(ionReader.newBytes()));
}
