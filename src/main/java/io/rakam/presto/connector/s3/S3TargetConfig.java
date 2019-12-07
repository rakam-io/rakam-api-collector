/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import static com.amazonaws.regions.Regions.DEFAULT_REGION;

public class S3TargetConfig
{
    private String accessKey;
    private String secretAccessKey;
    private String s3Bucket;
    private String region;
    private String endpoint;
    private DataSize maxDataSize = DataSize.succinctDataSize(256, DataSize.Unit.MEGABYTE);

    public String getRegion()
    {
        return region;
    }

    @Config("target.aws.region")
    public S3TargetConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    @NotNull
    public String getS3Bucket()
    {
        return s3Bucket;
    }

    @Config("target.aws.s3-bucket")
    public S3TargetConfig setS3Bucket(String s3Bucket)
    {
        this.s3Bucket = s3Bucket;
        return this;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("target.access-key")
    public S3TargetConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    @Config("target.secret-access-key")
    public S3TargetConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("target.aws.s3-endpoint")
    public S3TargetConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @Config("target.aws.s3-max-data-size")
    public S3TargetConfig setMaxDataSize(DataSize maxDataSize)
    {
        this.maxDataSize = maxDataSize;
        return this;
    }

    public DataSize getMaxDataSize() {
        return maxDataSize;
    }

    public AWSCredentialsProvider getCredentials()
    {
        if (accessKey == null && secretAccessKey == null) {
            return new InstanceProfileCredentialsProvider();
        }
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }
}
