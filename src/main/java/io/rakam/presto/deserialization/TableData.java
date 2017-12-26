/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.time.Instant;
import java.util.List;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public class TableData
{
    public final Page page;
    public final List<ColumnMetadata> metadata;

    public TableData(Page page, List<ColumnMetadata> metadata)
    {
        this.page = page;
        this.metadata = metadata;
    }
    
    public ExtractedPages extract(Instant instant, long gap) {
        IntArrayList previousDays = new IntArrayList(8);
        IntArrayList currentDay = new IntArrayList(8);

        int timeIdx = 0;
        for (; timeIdx < metadata.size(); timeIdx++) {
            if (metadata.get(timeIdx).getName().equals("_time")) {
                break;
            }
        }
        Block block = page.getBlock(timeIdx);

        long currentTime = instant.toEpochMilli();

        for (int i = 0; i < block.getPositionCount(); i++) {
            long eventTime = TIMESTAMP.getLong(block, i);
            if (currentTime - eventTime > gap) {
                previousDays.add(i);
            } else {
                currentDay.add(i);
            }
        }

        int channelCount = page.getChannelCount();
        Block[] previousDayBlocks = new Block[channelCount];
        Block[] currentDayBlocks = new Block[channelCount];

        for (int i = 0; i < channelCount; i++) {
            previousDayBlocks[i] = new DictionaryBlock(block, previousDays.toIntArray());
            currentDayBlocks[i] = new DictionaryBlock(block, currentDay.toIntArray());
        }

        Page prev = new Page(previousDayBlocks);
        Page now = new Page(currentDayBlocks);
        return new ExtractedPages(now, prev);
    }
    
    public static class ExtractedPages {
        public final Page now;
        public final Page prev;

        public ExtractedPages(Page now, Page prev) {
            this.now = now;
            this.prev = prev;
        }
    }
}
