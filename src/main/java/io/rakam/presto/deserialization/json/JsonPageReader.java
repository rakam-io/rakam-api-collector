/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.json;

import com.facebook.presto.spi.ColumnMetadata;
import io.rakam.presto.deserialization.PageReader;
import io.rakam.presto.deserialization.PageReaderDeserializer;

import java.io.IOException;
import java.util.List;

public class JsonPageReader
        extends PageReader<JsonDeserializer>
{
    public JsonPageReader(String checkpointColumn, List<ColumnMetadata> rakamSchema)
    {
        super(checkpointColumn, rakamSchema);
    }

    @Override
    public PageReaderDeserializer<JsonDeserializer> createReader()
    {
        return new JsonPageDeserializer(this);
    }

    public class JsonPageDeserializer
            implements PageReaderDeserializer<JsonDeserializer>
    {
        private final JsonPageReader jsonPageReader;

        public JsonPageDeserializer(JsonPageReader jsonPageReader)
        {
            this.jsonPageReader = jsonPageReader;
        }

        @Override
        public void read(JsonDeserializer in)
                throws IOException
        {
            in.deserialize(jsonPageReader);
        }
    }
}
