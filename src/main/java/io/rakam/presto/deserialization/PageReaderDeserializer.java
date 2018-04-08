/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import java.io.IOException;

public interface PageReaderDeserializer<T>
{
    void read(T in)
            throws IOException;

    // we will read data that has missing columns
    void setLastColumnIndex(int lastColumnIdx);

    void resetLastColumnIndex();
}
