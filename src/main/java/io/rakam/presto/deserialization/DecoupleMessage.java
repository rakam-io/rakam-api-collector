/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import java.io.IOException;

public interface DecoupleMessage<T> {
    boolean isRecentData(T record) throws IOException;
}
