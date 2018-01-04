/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import io.airlift.slice.BasicSliceInput;
import io.airlift.units.DataSize;

import java.io.IOException;

public class MemoryOrcDataSource
        extends AbstractOrcDataSource
{
    private final BasicSliceInput input;

    public MemoryOrcDataSource(String id, BasicSliceInput input, DataSize maxMergeDistance, DataSize maxBufferSize, DataSize streamBufferSize)
    {
        super(new OrcDataSourceId(id), input.length(), maxMergeDistance, maxBufferSize, streamBufferSize, true);
        this.input = input;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        this.input.setPosition(position);
        this.input.readFully(buffer, bufferOffset, bufferLength);
    }
}
