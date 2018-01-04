/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.json;
/*
 * Licensed under the Rakam Incorporation
 */
import com.facebook.presto.spi.SchemaTableName;

import java.io.IOException;

public interface JsonDeserializer {
    void deserialize(JsonPageReader jsonPageReader) throws IOException;

    void setData(byte[] value) throws IOException;

    SchemaTableName getTable() throws IOException;
}
