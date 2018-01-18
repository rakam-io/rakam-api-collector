/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.raptor;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import static com.amazonaws.regions.Regions.DEFAULT_REGION;

public class S3BackupConfig
{
    private String accessKey;
    private String secretAccessKey;
    private String s3Bucket;
    private String region;
    private String endpoint;

    public String getRegion()
    {
        return region;
    }

    @Config("raptor.aws.region")
    public S3BackupConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public Region getAWSRegion()
    {
        return Region.getRegion(region == null ? DEFAULT_REGION : Regions.fromName(region));
    }

    @NotNull
    public String getS3Bucket()
    {
        return s3Bucket;
    }

    @Config("raptor.aws.s3-bucket")
    public S3BackupConfig setS3Bucket(String s3Bucket)
    {
        this.s3Bucket = s3Bucket;
        return this;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("aws.access-key")
    public S3BackupConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    @Config("aws.secret-access-key")
    public S3BackupConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("raptor.aws.s3-endpoint")
    public S3BackupConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public AWSCredentialsProvider getCredentials()
    {
        if (accessKey == null && secretAccessKey == null) {
            return new InstanceProfileCredentialsProvider();
        }
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }
}