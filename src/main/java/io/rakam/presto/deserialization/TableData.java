/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.google.common.primitives.Ints;
import io.airlift.slice.SizeOf;
import io.rakam.presto.MemoryTracker;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public class TableData {
    public final Page page;
    public final List<ColumnMetadata> metadata;
    private static final long MILLIS_IN_DAY = ChronoUnit.DAYS.getDuration().toMillis();

    public TableData(Page page, List<ColumnMetadata> metadata) {
        this.page = page;
        this.metadata = metadata;

    }

    public ExtractedPages extract(LocalDate today, MemoryTracker memoryTracker) {
        Map<Integer, IntArrayList> previousDays = new HashMap<>();
        IntArrayList currentDay = new IntArrayList(page.getPositionCount());

        int timeIdx = 0;
        for (; timeIdx < metadata.size(); timeIdx++) {
            if (metadata.get(timeIdx).getName().equals("_time")) {
                break;
            }
        }
        Block block = page.getBlock(timeIdx);

        int currentDate = Ints.checkedCast(today.toEpochDay());

        for (int i = 0; i < block.getPositionCount(); i++) {
            int eventDate = Ints.checkedCast(TIMESTAMP.getLong(block, i) / MILLIS_IN_DAY);
            if (currentDate != eventDate) {
                IntArrayList integers = previousDays.get(eventDate);
                if(integers == null) {
                    integers = new IntArrayList(8);
                    previousDays.put(eventDate, integers);
                }

                integers.add(i);
            } else {
                currentDay.add(i);
            }
        }

        int channelCount = page.getChannelCount();
        Int2ObjectMap<Block[]> previousDaysBlocks = new Int2ObjectOpenHashMap<>(previousDays.size());
        for (Map.Entry<Integer, IntArrayList> entry : previousDays.entrySet()) {
            previousDaysBlocks.put(entry.getKey(), new Block[channelCount]);
        }

        Block[] currentDayBlocks = new Block[channelCount];

        int gap = 0;
        for (int i = 0; i < channelCount; i++) {
            for (Map.Entry<Integer, IntArrayList> entry : previousDays.entrySet()) {
                IntArrayList keys = entry.getValue();
                DictionaryBlock newBlock = new DictionaryBlock(keys.size(), block, keys.elements());
                previousDaysBlocks.get(entry.getKey())[i] = newBlock;
                gap += newBlock.getRetainedSizeInBytes() - block.getRetainedSizeInBytes();
            }

            DictionaryBlock newBlock = new DictionaryBlock(currentDay.size(), block, currentDay.elements());
            currentDayBlocks[i] = newBlock;
            gap += newBlock.getRetainedSizeInBytes() - block.getRetainedSizeInBytes();
        }

        Page now = new Page(currentDayBlocks);
        memoryTracker.reserveMemory(gap);

        Int2ObjectOpenHashMap<Page> previousDayPages = new Int2ObjectOpenHashMap<>(previousDays.size());
        for (Int2ObjectMap.Entry<Block[]> entry : previousDaysBlocks.int2ObjectEntrySet()) {
            previousDayPages.put(entry.getIntKey(), new Page(entry.getValue()));
        }
        return new ExtractedPages(now, previousDayPages);
    }

    public static class ExtractedPages {
        public final Page now;
        public final Int2ObjectMap<Page> prev;

        public ExtractedPages(Page now, Int2ObjectMap<Page> prev) {
            this.now = now;
            this.prev = prev;
        }
    }
}
