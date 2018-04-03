/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import com.facebook.presto.spi.ColumnMetadata;

import java.io.IOException;
import java.util.List;

public interface PageReaderDeserializer<T>
{
    void read(T in, List<ColumnMetadata> expectedSchema)
            throws IOException;
}
