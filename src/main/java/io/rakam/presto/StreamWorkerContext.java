/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.TableData;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Map;

public class StreamWorkerContext<T>
{
    private final MessageEventTransformer transformer;
    private final StreamConfig streamConfig;
    private final BasicMemoryBuffer.SizeCalculator<T> sizeCalculator;
    private final MemoryTracker memoryTracker;

    @Inject
    public StreamWorkerContext(MessageEventTransformer transformer, MemoryTracker memoryTracker, BasicMemoryBuffer.SizeCalculator sizeCalculator, StreamConfig streamConfig)
    {
        this.transformer = transformer;
        this.streamConfig = streamConfig;
        this.sizeCalculator = sizeCalculator;
        this.memoryTracker = memoryTracker;
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
