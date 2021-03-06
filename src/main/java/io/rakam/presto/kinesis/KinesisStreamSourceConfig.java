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
    private String dynamodbTable = "rakam-kinesis-worker";
    private Integer maxKinesisRecordsPerBatch;

    public String getStreamName()
    {
        return streamName;
    }

    @Config("kinesis.stream")
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }

    public String getDynamodbTable()
    {
        return dynamodbTable;
    }

    @Config("kinesis.max-records-per-batch")
    public void setMaxKinesisRecordsPerBatch(int maxKinesisRecordsPerBatch)
    {
        this.maxKinesisRecordsPerBatch = maxKinesisRecordsPerBatch;
    }

    public Integer getMaxKinesisRecordsPerBatch()
    {
        return maxKinesisRecordsPerBatch;
    }

    @Config("kinesis.consumer-dynamodb-table")
    public void setDynamodbTable(String dynamodbTable)
    {
        this.dynamodbTable = dynamodbTable;
    }

    public boolean getEnableCloudWatch()
    {
        return enableCloudWatch;
    }

    @Config("aws.enable-cloudwatch")
    public KinesisStreamSourceConfig setEnableCloudWatch(boolean enableCloudWatch)
    {
        this.enableCloudWatch = enableCloudWatch;
        return this;
    }

    public String getKinesisEndpoint()
    {
        return kinesisEndpoint;
    }

    @Config("aws.kinesis-endpoint")
    public KinesisStreamSourceConfig setKinesisEndpoint(String kinesisEndpoint)
    {
        this.kinesisEndpoint = kinesisEndpoint;
        return this;
    }

    public String getDynamodbEndpoint()
    {
        return dynamodbEndpoint;
    }

    @Config("aws.dynamodb-endpoint")
    public KinesisStreamSourceConfig setDynamodbEndpoint(String dynamodbEndpoint)
    {
        this.dynamodbEndpoint = dynamodbEndpoint;
        return this;
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

    @Config("aws.access-key")
    public KinesisStreamSourceConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    @Config("aws.secret-access-key")
    public KinesisStreamSourceConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
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
