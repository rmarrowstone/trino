package io.trino.hive.formats.ion;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;

import static io.trino.spi.type.IntegerType.INTEGER;

public class IonEncoderFactory
{
    public static IonEncoder buildEncoder(List<Column> columns)
    {
        return RowEncoder.forFields(columns.stream()
                .map(c -> new RowType.Field(Optional.of(c.name()), c.type()))
                .toList());
    }

    private interface BlockEncoder
    {
        void encode(IonWriter writer, Block block, int position) throws IOException;
    }

    private static BlockEncoder encoderForType(Type type)
    {
        return switch (type) {
            case IntegerType _ -> intEncoder;
            case BooleanType _ -> boolEncoder;
            case VarcharType _ -> stringEncoder;
            case VarbinaryType _ -> binaryEncoder;
            case RowType t -> RowEncoder.forFields(t.getFields()); // remember to wrap element encoder here
            case ArrayType t -> new ArrayEncoder(wrapEncoder(encoderForType(t.getElementType()))); // remember to wrap element encoder here
            default -> throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
        };
    }

    private static BlockEncoder wrapEncoder(BlockEncoder encoder)
    {
        return (writer, block, position) ->
        {
            if (block.isNull(position)) {
                writer.writeNull();
            } else {
                encoder.encode(writer, block, position);
            }
        };
    }

    private record RowEncoder(List<String> fieldNames, List<BlockEncoder> fieldEncoders)
            implements BlockEncoder, IonEncoder
    {
        private static RowEncoder forFields(List<RowType.Field> fields) {
            ImmutableList.Builder<String> fieldNamesBuilder = ImmutableList.builder();
            ImmutableList.Builder<BlockEncoder> fieldEncodersBuilder = ImmutableList.builder();

            for (RowType.Field field : fields) {
                fieldNamesBuilder.add(field.getName().get());
                fieldEncodersBuilder.add(wrapEncoder(encoderForType(field.getType())));
            }

            return new RowEncoder(fieldNamesBuilder.build(), fieldEncodersBuilder.build());
        }

        @Override
        public void encode(IonWriter writer, Block block, int position)
                throws IOException
        {
            encodeStruct(writer, ((RowBlock) block)::getFieldBlock, position);
        }

        @Override
        public void encode(IonWriter writer, Page page)
                throws IOException
        {
            for (int i = 0; i < page.getPositionCount(); i++) {
                System.err.println("wrote row as Ion");
                encodeStruct(writer, page::getBlock, i);
            }
            // todo: it's probably preferable to decouple ion writer flushes
            //       from page sizes, but it's convenient for now
            System.err.println("flushing IonWriter after page");
            writer.flush();
        }

        private void encodeStruct(IonWriter writer, IntFunction<Block> blockSelector, int position)
                throws IOException
        {
            writer.stepIn(IonType.STRUCT);
            for (int i = 0; i < fieldEncoders.size(); i++) {
                // todo: it may be preferable to elide struct fields when null
                //       consider what the Ion Hive Serde does

                writer.setFieldName(fieldNames.get(i));
                fieldEncoders.get(i)
                        .encode(writer, blockSelector.apply(i), position);
            }
            writer.stepOut();
        }
    }

    private record ArrayEncoder(BlockEncoder elementEncoder)
            implements BlockEncoder
        {

            @Override
            public void encode(IonWriter writer, Block block, int position)
                    throws IOException
            {
                writer.stepIn(IonType.LIST);
                Block elementBlock = ((ArrayBlock) block).getArray(position);
                for (int i = 0; i < elementBlock.getPositionCount(); i++) {
                    elementEncoder.encode(writer, elementBlock, i);
                }
                writer.stepOut();
            }
        }

    private static final BlockEncoder intEncoder = (writer, block, position) ->
            writer.writeInt(INTEGER.getInt(block, position));

    private static final BlockEncoder stringEncoder = (writer, block, position) ->
            writer.writeString(VarcharType.VARCHAR.getSlice(block, position).toString(StandardCharsets.UTF_8));

    private static final BlockEncoder boolEncoder = (writer, block, position) ->
            writer.writeBool(BooleanType.BOOLEAN.getBoolean(block, position));

    private static final BlockEncoder binaryEncoder = (writer, block, position) ->
            writer.writeBlob(VarbinaryType.VARBINARY.getSlice(block, position).getBytes());
}
