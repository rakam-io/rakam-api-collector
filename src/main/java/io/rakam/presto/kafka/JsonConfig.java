/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import io.airlift.configuration.Config;
import io.rakam.presto.deserialization.json.FabricJsonDeserializer;
import io.rakam.presto.deserialization.json.JsonDeserializer;
import io.rakam.presto.deserialization.json.RakamJsonDeserializer;

public class JsonConfig
{
    private JsonFormat dataLayout = JsonFormat.FABRIC;

    public JsonFormat getDataLayout()
    {
        return dataLayout;
    }

    @Config("source.data-format.json.layout")
    public JsonConfig setDataLayout(JsonFormat dataLayout)
    {
        this.dataLayout = dataLayout;
        return this;
    }

    public enum JsonFormat
    {
        RAKAM(RakamJsonDeserializer.class), FABRIC(FabricJsonDeserializer.class);

        private final Class<? extends JsonDeserializer> jsonDeserializerClass;

        JsonFormat(Class<? extends JsonDeserializer> jsonDeserializerClass)
        {
            this.jsonDeserializerClass = jsonDeserializerClass;
        }

        public Class<? extends JsonDeserializer> getJsonDeserializerClass()
        {
            return jsonDeserializerClass;
        }
    }
}
