/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;

import javax.validation.constraints.Size;

import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class KafkaConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;
    private static final int ZOOKEEPER_DEFAULT_PORT = 2181;

    private Set<HostAddress> nodes;
    private Set<HostAddress> zkNodes;

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

    private static HostAddress toKafkaHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }

    private static HostAddress toZookeeperHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(ZOOKEEPER_DEFAULT_PORT);
    }
}
