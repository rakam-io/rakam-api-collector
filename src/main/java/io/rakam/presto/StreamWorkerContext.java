/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.common.collect.Table;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.TableData;

import javax.inject.Inject;

import java.io.IOException;

public class StreamWorkerContext<T>
{
    private final MessageEventTransformer transformer;
    private final StreamConfig streamConfig;

    @Inject
    public StreamWorkerContext(MessageEventTransformer transformer, StreamConfig streamConfig)
    {
        this.transformer = transformer;
        this.streamConfig = streamConfig;
    }

    public void shutdown()
    {
    }

    public Table<String, String, TableData> convert(Iterable<? extends T> records, Iterable<? extends T> bulkRecords)
            throws IOException
    {
        return transformer.createPageTable(records, bulkRecords);
    }

    public BasicMemoryBuffer createBuffer()
    {
        return new BasicMemoryBuffer(streamConfig);
    }
}
