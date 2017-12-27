/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;

import javax.validation.constraints.Size;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class KafkaConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;
    private static final int ZOOKEEPER_DEFAULT_PORT = 2181;
    private DataFormat dataFormat = DataFormat.AVRO;

    private Set<HostAddress> nodes;
    private Set<HostAddress> zkNodes;
    private String[] topic;
    private String offset = "latest";
    private String groupId = "presto_streaming";

    private String sessionTimeOut = "12000";
    private String requestTimeOut = "15000";

    public List<String> getTopic()
    {
        return ImmutableList.copyOf(topic);
    }


    public String getSessionTimeOut()
    {
        return sessionTimeOut;
    }

    public String getRequestTimeOut()
    {
        return requestTimeOut;
    }

    public String getOffset()
    {
        return offset;
    }

    public String getGroupId()
    {
        return groupId;
    }

    @Config("kafka.topic")
    public KafkaConfig setTopic(String topic)
    {
        if (topic != null) {
            topic = topic.replaceAll("\\s+", "");
            this.topic = topic.split(",");
        }
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("kafka.session.timeout.ms")
    public KafkaConfig setSessionTimeOut(String sessionTimeOut)
    {
        if (sessionTimeOut != null) {
            this.sessionTimeOut = sessionTimeOut;
        }
        return this;
    }

    @Config("kafka.request.timeout.ms")
    public KafkaConfig setRequestTimeOut(String requestTimeOut)
    {
        if (requestTimeOut != null) {
            this.requestTimeOut = requestTimeOut;
        }
        return this;
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

    @Config("kafka.offset")
    public KafkaConfig setOffset(String offset)
    {
        if (offset != null) {
            this.offset = offset;
        }
        return this;
    }

    @Config("kafka.group.id")
    public KafkaConfig setGroupId(String groupId)
    {
        if (groupId != null) {
            this.groupId = groupId;
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

    @Config("source.data-format")
    public KafkaConfig setDataFormat(DataFormat dataFormat)
    {
        this.dataFormat = dataFormat;
        return this;
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

    public enum DataFormat
    {
        JSON, AVRO
    }
}
