package io.rakam.presto.deserialization;

/*
 * Licensed under the Rakam Incorporation
 */

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.block.BlockBuilderStatus.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class PageBuilder
{
    // We choose default initial size to be 8 for PageBuilder and BlockBuilder
    // so the underlying data is larger than the object overhead, and the size is power of 2.
    //
    // This could be any other small number.
    private static final int DEFAULT_INITIAL_EXPECTED_ENTRIES = 8;

    private final BlockBuilder[] blockBuilders;
    private final List<Type> types;
    private PageBuilderStatus pageBuilderStatus;
    private int declaredPositions;

    /**
     * Create a PageBuilder with given types.
     * <p>
     * A PageBuilder instance created with this constructor has no estimation about bytes per entry,
     * therefore it can resize frequently while appending new rows.
     * <p>
     * This constructor should only be used to get the initial PageBuilder.
     * Once the PageBuilder is full use reset() or createPageBuilderLike() to create a new
     * PageBuilder instance with its size estimated based on previous data.
     */
    public PageBuilder(List<? extends Type> types)
    {
        this(DEFAULT_INITIAL_EXPECTED_ENTRIES, types);
    }

    public PageBuilder(int initialExpectedEntries, List<? extends Type> types)
    {
        this(initialExpectedEntries, DEFAULT_MAX_PAGE_SIZE_IN_BYTES, types, Optional.empty());
    }

    public static PageBuilder withMaxPageSize(int maxPageBytes, List<? extends Type> types)
    {
        return new PageBuilder(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxPageBytes, types, Optional.empty());
    }

    public PageBuilder(int initialExpectedEntries, int maxPageBytes, List<? extends Type> types, Optional<BlockBuilder[]> templateBlockBuilders)
    {
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));

        pageBuilderStatus = new PageBuilderStatus(maxPageBytes, DEFAULT_MAX_BLOCK_SIZE_IN_BYTES);
        blockBuilders = new BlockBuilder[types.size()];

        if (templateBlockBuilders.isPresent()) {
            this.declaredPositions = initialExpectedEntries;
            BlockBuilder[] templates = templateBlockBuilders.get();
            checkArgument(templates.length == types.size(), "Size of templates and types should match");
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = templates[i].newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
            }
        }
        else {
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = types.get(i).createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), initialExpectedEntries);
            }
        }
    }

    public void reset()
    {
        if (isEmpty()) {
            return;
        }
        pageBuilderStatus = new PageBuilderStatus(pageBuilderStatus.getMaxPageSizeInBytes(), DEFAULT_MAX_BLOCK_SIZE_IN_BYTES);

        declaredPositions = 0;

        for (int i = 0; i < types.size(); i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
        }
    }

    public PageBuilder newPageBuilderLike()
    {
        return new PageBuilder(declaredPositions, pageBuilderStatus.getMaxPageSizeInBytes(), types, Optional.of(blockBuilders));
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
        // We use a foreach loop instead of streams
        // as it has much better performance.
        long retainedSizeInBytes = 0;
        for (BlockBuilder blockBuilder : blockBuilders) {
            retainedSizeInBytes += blockBuilder.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
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
                throw new IllegalStateException(format("Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount()));
            }
        }

        return new Page(blocks);
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

        PageBuilder pageBuilder = new PageBuilder(declaredPositions,
                DEFAULT_MAX_PAGE_SIZE_IN_BYTES,
                ImmutableList.<Type>builder().addAll(types).add(type).build(),
                Optional.of(newBlockBuilders));
        return pageBuilder;
    }
}