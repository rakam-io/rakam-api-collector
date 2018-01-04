/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.airlift.configuration.Config;

public class KinesisStreamSourceConfig
{
    private String accessKey;
    private String secretAccessKey;
    private String streamName;
    private String region;
    private String kinesisEndpoint;
    private String dynamodbEndpoint;
    private boolean enableCloudWatch = true;
    private String dynamodbTable;

    public String getStreamName()
    {
        return streamName;
    }

    @Config("kinesis.stream")
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }

    @Config("kinesis.consumer-dynamodb-table")
    public void setDynamodbTable(String dynamodbTable)
    {
        this.dynamodbTable = dynamodbTable;
    }

    public String getDynamodbTable()
    {
        return dynamodbTable;
    }

    @Config("aws.access-key")
    public KinesisStreamSourceConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @Config("aws.enable-cloudwatch")
    public KinesisStreamSourceConfig setEnableCloudWatch(boolean enableCloudWatch)
    {
        this.enableCloudWatch = enableCloudWatch;
        return this;
    }

    public boolean getEnableCloudWatch()
    {
        return enableCloudWatch;
    }

    @Config("aws.kinesis-endpoint")
    public KinesisStreamSourceConfig setKinesisEndpoint(String kinesisEndpoint)
    {
        this.kinesisEndpoint = kinesisEndpoint;
        return this;
    }

    public String getKinesisEndpoint()
    {
        return kinesisEndpoint;
    }

    @Config("aws.dynamodb-endpoint")
    public KinesisStreamSourceConfig setDynamodbEndpoint(String dynamodbEndpoint)
    {
        this.dynamodbEndpoint = dynamodbEndpoint;
        return this;
    }

    public String getDynamodbEndpoint()
    {
        return dynamodbEndpoint;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("aws.region")
    public KinesisStreamSourceConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("aws.secret-access-key")
    public KinesisStreamSourceConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    public AWSCredentialsProvider getCredentials()
    {
        if (accessKey == null && secretAccessKey == null) {
            return new InstanceProfileCredentialsProvider();
        }
        return new StaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }

    public Region getAWSRegion()
    {
        return Region.getRegion(region == null ? Regions.DEFAULT_REGION : Regions.fromName(region));
    }
}
