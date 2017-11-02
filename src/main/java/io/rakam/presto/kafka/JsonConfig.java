/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import io.airlift.configuration.Config;
import io.rakam.presto.deserialization.json.JsonDeserializer;
import io.rakam.presto.deserialization.json.RakamJsonDeserializer;

public class JsonConfig {
    private JsonFormat dataLayout = JsonFormat.RAKAM;

    @Config("source.data-format.json.layout")
    public JsonConfig setDataFormat(JsonFormat dataLayout) {
        this.dataLayout = dataLayout;
        return this;
    }

    public JsonFormat getDataLayout() {
        return dataLayout;
    }

    public enum JsonFormat {
        RAKAM(RakamJsonDeserializer.class);

        private final Class<? extends JsonDeserializer> jsonDeserializerClass;

        JsonFormat(Class<? extends JsonDeserializer> jsonDeserializerClass) {
            this.jsonDeserializerClass = jsonDeserializerClass;
        }

        public Class<? extends JsonDeserializer> getJsonDeserializerClass() {
            return jsonDeserializerClass;
        }
    }
}
