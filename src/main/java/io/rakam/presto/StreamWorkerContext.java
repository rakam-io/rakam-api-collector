/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.common.base.Throwables;
import com.google.common.collect.Table;

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

    public Table<String, String, MessageEventTransformer.TableData> convert(Iterable<? extends T> records, Iterable<? extends T> bulkRecords)
            throws IOException
    {
        Table<String, String, MessageEventTransformer.TableData> pages;
        try {
            pages = transformer.createPageTable(records, bulkRecords);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return pages;
    }

    public BasicMemoryBuffer createBuffer()
    {
        return new BasicMemoryBuffer(streamConfig);
    }
}
