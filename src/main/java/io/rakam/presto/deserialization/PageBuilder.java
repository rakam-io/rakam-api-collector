/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.spi.block.BlockBuilderStatus.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class PageBuilder
{
    private final BlockBuilder[] blockBuilders;
    private final List<Type> types;
    private PageBuilderStatus pageBuilderStatus;
    private int declaredPositions;

    public PageBuilder(List<? extends Type> types)
    {
        this(Integer.MAX_VALUE, types);
    }

    public PageBuilder(int initialExpectedEntries, List<? extends Type> types)
    {
        this(initialExpectedEntries, types, Optional.empty(), 0);
    }

    public PageBuilder(int initialExpectedEntries, List<? extends Type> types, Optional<BlockBuilder[]> templateBlockBuilders, Integer declaredPositions)
    {
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));

        int maxBlockSizeInBytes;
        if (!types.isEmpty()) {
            maxBlockSizeInBytes = (int) (1.0 * DEFAULT_MAX_PAGE_SIZE_IN_BYTES / types.size());
            maxBlockSizeInBytes = Math.min(DEFAULT_MAX_BLOCK_SIZE_IN_BYTES, maxBlockSizeInBytes);
        }
        else {
            maxBlockSizeInBytes = 0;
        }
        pageBuilderStatus = new PageBuilderStatus(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, maxBlockSizeInBytes);
        blockBuilders = new BlockBuilder[types.size()];

        if (declaredPositions != null) {
            this.declaredPositions = declaredPositions;
        }

        if (templateBlockBuilders.isPresent()) {
            BlockBuilder[] templates = templateBlockBuilders.get();
            checkArgument(templates.length == types.size(), "Size of templates and types should match");
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = templates[i];
            }
        }
        else {
            int expectedEntries = Math.min(maxBlockSizeInBytes, initialExpectedEntries);
            for (Type type : types) {
                if (type instanceof FixedWidthType) {
                    int fixedSize = Math.max(((FixedWidthType) type).getFixedSize(), 1);
                    expectedEntries = Math.min(expectedEntries, maxBlockSizeInBytes / fixedSize);
                }
                else {
                    // We really have no idea how big these are going to be, so just guess. In reset() we'll make a better guess
                    expectedEntries = Math.min(expectedEntries, maxBlockSizeInBytes / 32);
                }
            }
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = types.get(i).createBlockBuilder(
                        pageBuilderStatus.createBlockBuilderStatus(),
                        expectedEntries,
                        pageBuilderStatus.getMaxBlockSizeInBytes() / expectedEntries);
            }
        }
    }

    private static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public PageBuilder newPageBuilderWithType(Type type)
    {
        int newLength = types.size() + 1;
        BlockBuilder[] newBlockBuilders = new BlockBuilder[newLength];
        for (int i = 0; i < blockBuilders.length; i++) {
            newBlockBuilders[i] = blockBuilders[i];
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), declaredPositions * 2);
        newBlockBuilders[newBlockBuilders.length - 1] = blockBuilder;
        int oldIterations = declaredPositions - 1;
        for (int i = 0; i < oldIterations; i++) {
            blockBuilder.appendNull();
        }

        PageBuilder pageBuilder = new PageBuilder(Integer.MAX_VALUE,
                ImmutableList.<Type>builder().addAll(types).add(type).build(),
                Optional.of(newBlockBuilders), declaredPositions);
        return pageBuilder;
    }

    public void reset()
    {
        if (isEmpty()) {
            return;
        }
        pageBuilderStatus = new PageBuilderStatus(pageBuilderStatus.getMaxPageSizeInBytes(), pageBuilderStatus.getMaxBlockSizeInBytes());

        declaredPositions = 0;

        for (int i = 0; i < types.size(); i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
        }
    }

    public BlockBuilder getBlockBuilder(int channel)
    {
        return blockBuilders[channel];
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    public void declarePosition()
    {
        declaredPositions++;
    }

    public void declarePositions(int positions)
    {
        declaredPositions += positions;
    }

    public boolean isFull()
    {
        return declaredPositions == Integer.MAX_VALUE || pageBuilderStatus.isFull();
    }

    public boolean isEmpty()
    {
        return declaredPositions == 0;
    }

    public int getPositionCount()
    {
        return declaredPositions;
    }

    public long getSizeInBytes()
    {
        return pageBuilderStatus.getSizeInBytes();
    }

    public long getRetainedSizeInBytes()
    {
        return Stream.of(blockBuilders).mapToLong(BlockBuilder::getRetainedSizeInBytes).sum();
    }

    public Page build()
    {
        if (blockBuilders.length == 0) {
            return new Page(declaredPositions);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
            if (blocks[i].getPositionCount() != declaredPositions) {
                throw new IllegalStateException(String.format("Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount()));
            }
        }

        return new Page(blocks);
    }
}
