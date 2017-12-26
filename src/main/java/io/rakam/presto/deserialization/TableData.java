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
}
