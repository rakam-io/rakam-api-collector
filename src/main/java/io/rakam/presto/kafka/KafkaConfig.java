/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.rakam.presto.StreamConfig;

import javax.validation.constraints.Size;

import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class KafkaConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;
    private static final int ZOOKEEPER_DEFAULT_PORT = 2181;
    private DataFormat dataFormat = DataFormat.AVRO;

    private Set<HostAddress> nodes;
    private Set<HostAddress> zkNodes;
    private String topic;

    public String getTopic()
    {
        return topic;
    }

    @Config("kafka.topic")
    public KafkaConfig setTopic(String topic)
    {
        this.topic = topic;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("kafka.nodes")
    public KafkaConfig setNodes(String nodes)
    {
        if (nodes != null) {
            Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
            this.nodes = ImmutableSet.copyOf(transform(splitter.split(nodes), KafkaConfig::toKafkaHostAddress));
        }
        else {
            this.nodes = null;
        }
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getZookeeperNodes()
    {
        return zkNodes;
    }

    @Config("zookeeper.nodes")
    public KafkaConfig setZookeeperNodes(String nodes)
    {
        if (nodes != null) {
            Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
            this.zkNodes = ImmutableSet.copyOf(transform(splitter.split(nodes), KafkaConfig::toZookeeperHostAddress));
        }
        else {
            this.zkNodes = null;
        }
        return this;
    }

    @Config("data-format")
    public void setDataFormat(DataFormat dataFormat)
    {
        this.dataFormat = dataFormat;
    }

    public DataFormat getDataFormat()
    {
        return dataFormat;
    }


    private static HostAddress toKafkaHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }

    private static HostAddress toZookeeperHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(ZOOKEEPER_DEFAULT_PORT);
    }

    public enum DataFormat {
        JSON, dataFormat, AVRO
    }
}
