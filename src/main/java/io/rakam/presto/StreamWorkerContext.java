/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.PageReader;
import io.rakam.presto.deserialization.TableData;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Map;

public class StreamWorkerContext<T>
{
    private final MessageEventTransformer transformer;
    private final StreamConfig streamConfig;
    private final MemoryTracker memoryTracker;
    private final BasicMemoryBuffer.SizeCalculator<T> sizeCalculator;

    @Inject
    public StreamWorkerContext(MessageEventTransformer transformer, BasicMemoryBuffer.SizeCalculator sizeCalculator, MemoryTracker memoryTracker, StreamConfig streamConfig)
    {
        this.transformer = transformer;
        this.streamConfig = streamConfig;
        this.memoryTracker = memoryTracker;
        this.sizeCalculator = sizeCalculator;
    }

    public void shutdown()
    {
    }

    public Map<SchemaTableName, TableData> convert(Iterable<? extends T> records, Iterable<? extends T> bulkRecords)
            throws IOException
    {
        return transformer.createPageTable(records, bulkRecords);
    }

    public BasicMemoryBuffer createBuffer()
    {
        return new BasicMemoryBuffer(streamConfig, memoryTracker, sizeCalculator);
    }
}
