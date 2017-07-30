/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;

import java.util.List;

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
