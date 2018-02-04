/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.raptor.RaptorErrorCode;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.rakam.presto.MemoryTracker;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.orc.OrcWriter.DEFAULT_DICTIONARY_MEMORY_MAX_SIZE;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_ROW_GROUP_MAX_ROW_COUNT;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_STRIPE_MAX_ROW_COUNT;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_STRIPE_MAX_SIZE;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_STRIPE_MIN_ROW_COUNT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.jsonCodec;

public class InMemoryOrcFileWriter
        implements Closeable
{
    private PageBuilder pageBuilder;
    private final OrcWriter orcWriter;
    private final InMemoryBuffer buffer;
    private final MemoryTracker memoryTracker;
    private final ImmutableList<Type> storageTypes;

    private boolean closed;
    private long rowCount;
    private long uncompressedSize;
    private long writerRetainedSize = 0;

    public InMemoryOrcFileWriter(
            List<Long> columnIds,
            List<Type> columnTypes,
            File target,
            MemoryTracker memoryTracker,
            TypeManager typeManager,
            InMemoryBuffer buffer)
    {
        StorageTypeConverter converter = new StorageTypeConverter(typeManager);
        this.storageTypes = columnTypes.stream()
                .map(converter::toStorageType)
                .collect(toImmutableList());

        this.buffer = buffer;
        this.memoryTracker = memoryTracker;

        Map<String, String> metadata = createFileMetadata(columnIds, columnTypes);
        orcWriter = createOrcFileWriter(target, columnIds, storageTypes, metadata);
        long retainedBytes = orcWriter.getRetainedBytes();
        writerRetainedSize = retainedBytes;
        memoryTracker.reserveMemory(retainedBytes);
    }

    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            appendPage(page);
        }
    }

    public void appendPages(List<Page> pages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");
        if (pageBuilder == null) {
            pageBuilder = new PageBuilder(storageTypes);
        }

        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = pages.get(pageIndexes[i]);
            int position = positionIndexes[i];

            pageBuilder.declarePosition();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Type type = pageBuilder.getType(channel);
                Block block = page.getBlock(channel);
                BlockBuilder output = pageBuilder.getBlockBuilder(channel);
                type.appendTo(block, position, output);
            }

            if (pageBuilder.isFull()) {
                appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            orcWriter.close();
        }
        catch (IOException e) {
            throw new PrestoException(RaptorErrorCode.RAPTOR_ERROR, "Failed to close writer", e);
        }

        memoryTracker.freeMemory(orcWriter.getRetainedBytes());
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    private void appendPage(Page page)
    {
        rowCount += page.getPositionCount();
        uncompressedSize += page.getSizeInBytes();

        try {
            orcWriter.write(page);
        }
        catch (IOException e) {
            throw new PrestoException(RaptorErrorCode.RAPTOR_ERROR, "Failed to write data", e);
        }

        long retainedBytes = orcWriter.getRetainedBytes();
        memoryTracker.reserveMemory(retainedBytes - writerRetainedSize);
    }

    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    public static Map<String, String> createFileMetadata(List<Long> columnIds, List<Type> columnTypes)
    {
        ImmutableMap.Builder<Long, TypeSignature> columnTypesMap = ImmutableMap.builder();
        for (int i = 0; i < columnIds.size(); i++) {
            columnTypesMap.put(columnIds.get(i), columnTypes.get(i).getTypeSignature());
        }
        OrcFileMetadata metadata = new OrcFileMetadata(columnTypesMap.build());

        return ImmutableMap.of(OrcFileMetadata.KEY, METADATA_CODEC.toJson(metadata));
    }

    public OrcWriter createOrcFileWriter(
            File target,
            List<Long> columnIds,
            List<Type> storageTypes,
            Map<String, String> metadatas)
    {
        checkArgument(columnIds.size() == storageTypes.size(), "ids and types mismatch");
        checkArgument(columnIds.stream().distinct().count() == columnIds.size(), "ids must be unique");

        List<String> columnNames = columnIds.stream()
                .map(Object::toString)
                .collect(toImmutableList());

        TrackedDynamicSliceOutput output = buffer.create(target.getName(), columnIds.size() * 5000);

        return OrcWriter.createOrcWriter(
                output,
                columnNames,
                storageTypes, CompressionKind.SNAPPY, DEFAULT_STRIPE_MAX_SIZE,
                DEFAULT_STRIPE_MIN_ROW_COUNT,
                DEFAULT_STRIPE_MAX_ROW_COUNT,
                DEFAULT_ROW_GROUP_MAX_ROW_COUNT,
                DEFAULT_DICTIONARY_MEMORY_MAX_SIZE,
                metadatas,
                DateTimeZone.UTC, false);
    }
}