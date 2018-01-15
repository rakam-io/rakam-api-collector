/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import java.io.IOException;

public interface DecoupleMessage<T>
{
    void read(T record, RecordData recordData)
            throws IOException;

    class RecordData {
        public String collection;
        public int date;
    }
}
