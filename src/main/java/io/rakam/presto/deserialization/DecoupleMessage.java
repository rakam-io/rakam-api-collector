/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import java.io.IOException;

public interface DecoupleMessage<T>
{
    int getDateOfRecord(T record)
            throws IOException;
}
