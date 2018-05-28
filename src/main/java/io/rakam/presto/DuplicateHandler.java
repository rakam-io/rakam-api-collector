/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import java.io.IOException;

public interface DuplicateHandler<T>
{
    boolean isUnique(T record)
            throws IOException;
}
