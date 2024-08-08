package io.trino.hive.formats;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractType;
import io.trino.spi.type.TypeSignature;

public class NoopType
        extends AbstractType
{
    public static final NoopType NOOP = new NoopType();

    protected NoopType()
    {
        super(new TypeSignature("noop"), boolean.class, ByteArrayBlock.class);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return null;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return null;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return null;
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {

    }

    @Override
    public int getFlatFixedSize()
    {
        return 0;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return false;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        return 0;
    }

    @Override
    public int relocateFlatVariableWidthOffsets(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        return 0;
    }
}
