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
import io.trino.hive.formats.line.Column;
import io.trino.spi.PageBuilder;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
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
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IonDecoderFactory
{
    private IonDecoderFactory() {}

    /**
     * Builds a decoder for the given columns.
     * <p>
     * The decoder expects to decode the _current_ Ion Value.
     * It also expects that the calling code will manage the PageBuilder.
     * <p>
     */
    public static IonDecoder buildDecoder(List<Column> columns)
    {
        return RowDecoder.forFields(
                columns.stream()
                        .map(c -> new RowType.Field(Optional.of(c.name()), c.type()))
                        .toList());
    }

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
            case DecimalType t -> wrapDecoder(decimalDecoder(t), IonType.DECIMAL, IonType.INT);
            case VarcharType _, CharType _ -> wrapDecoder(stringDecoder, IonType.STRING, IonType.SYMBOL);
            case VarbinaryType _ -> wrapDecoder(binaryDecoder, IonType.BLOB, IonType.CLOB);
            case RowType r -> wrapDecoder(RowDecoder.forFields(r.getFields()), IonType.STRUCT);
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
     * Class is both the Top-Level-Value Decoder and the Row Decoder for nested
     * structs.
     */
    private record RowDecoder(Map<String, Integer> fieldPositions, List<BlockDecoder> fieldDecoders)
            implements IonDecoder, BlockDecoder
    {
        private static RowDecoder forFields(List<RowType.Field> fields)
        {
            ImmutableList.Builder<BlockDecoder> decoderBuilder = ImmutableList.builder();
            ImmutableMap.Builder<String, Integer> fieldPositionBuilder = ImmutableMap.builder();
            IntStream.range(0, fields.size())
                    .forEach(position -> {
                        RowType.Field field = fields.get(position);
                        decoderBuilder.add(decoderForType(field.getType()));
                        fieldPositionBuilder.put(field.getName().get().toLowerCase(Locale.ROOT), position);
                    });

            return new RowDecoder(fieldPositionBuilder.buildOrThrow(), decoderBuilder.build());
        }

        @Override
        public void decode(IonReader ionReader, PageBuilder pageBuilder)
        {
            // todo: we could also map an Ion List to a Struct
            if (ionReader.getType() != IonType.STRUCT) {
                throw new IonException("RowType must be Structs! Encountered: " + ionReader.getType());
            }
            if (ionReader.isNullValue()) {
                // todo: is this an error or just a null value?
                //       i think in the hive serde it's a null record.
                throw new IonException("Top Level Values must not be null!");
            }
            decode(ionReader, pageBuilder::getBlockBuilder);
        }

        @Override
        public void decode(IonReader ionReader, BlockBuilder blockBuilder)
        {
            ((RowBlockBuilder) blockBuilder)
                    .buildEntry(fieldBuilders -> decode(ionReader, fieldBuilders::get));
        }

        // assumes that the reader is positioned on a non-null struct value
        private void decode(IonReader ionReader, IntFunction<BlockBuilder> blockSelector)
        {
            boolean[] encountered = new boolean[fieldDecoders.size()];
            ionReader.stepIn();

            while (ionReader.next() != null) {
                // todo: case insensitivity
                final Integer fieldIndex = fieldPositions.get(ionReader.getFieldName().toLowerCase(Locale.ROOT));
                if (fieldIndex == null) {
                    continue;
                }
                final BlockBuilder blockBuilder = blockSelector.apply(fieldIndex);
                if (encountered[fieldIndex]) {
                    blockBuilder.resetTo(blockBuilder.getPositionCount() - 1);
                }
                else {
                    encountered[fieldIndex] = true;
                }
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
        return (reader, builder) -> {
            try {
                BigDecimal decimalMicros = reader.timestampValue().getDecimalMillis()
                        .setScale(type.getPrecision() - 3, RoundingMode.UNNECESSARY)
                        .movePointRight(3);

                if (type.isShort()) {
                    type.writeLong(builder, decimalMicros.longValue());
                }
                else {
                    BigDecimal subMicrosFrac = decimalMicros.remainder(BigDecimal.ONE)
                            .movePointRight(6);
                    type.writeObject(builder, new LongTimestamp(decimalMicros.longValue(), subMicrosFrac.intValue()));
                }
            }
            catch (ArithmeticException e) {
                throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR,
                        "Timestamp value %s is too precise for %d digits of fractional seconds!"
                                .formatted(reader.timestampValue(), type.getPrecision()));
            }
        };
    }

    private static BlockDecoder decimalDecoder(DecimalType type)
    {
        return (reader, builder) -> {
            try {
                BigInteger unscaled = reader.bigDecimalValue()
                        .setScale(type.getScale(), RoundingMode.UNNECESSARY)
                        .unscaledValue();

                if (Decimals.overflows(unscaled, type.getPrecision())) {
                    throw new TrinoException(StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE,
                            "Decimal value %s does not fit %d digits of precision and %d of scale!"
                                    .formatted(reader.bigDecimalValue(), type.getPrecision(), type.getScale()));
                }
                if (type.isShort()) {
                    type.writeLong(builder, unscaled.longValue());
                }
                else {
                    type.writeObject(builder, Int128.valueOf(unscaled));
                }
            }
            catch (ArithmeticException e) {
                throw new TrinoException(StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE,
                        "Decimal value %s does not fit %d digits of scale!"
                                .formatted(reader.bigDecimalValue(), type.getScale()));
            }
        };
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
