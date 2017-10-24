/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.airlift.configuration.Config;

public class S3MiddlewareConfig
{
    private String s3Bucket;
    private String secretAccessKey;
    private String region;
    private String accessKey;
    private String endpoint;

    @Config("aws.s3-bulk-bucket")
    public void setS3Bucket(String s3Bucket)
    {
        this.s3Bucket = s3Bucket;
    }

    public String getS3Bucket()
    {
        return s3Bucket;
    }

    @Config("aws.access-key")
    public S3MiddlewareConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("aws.region")
    public S3MiddlewareConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("aws.secret-access-key")
    public S3MiddlewareConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    @Config("aws.s3-endpoint")
    public S3MiddlewareConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    public AWSCredentialsProvider getCredentials()
    {
        if(accessKey == null && secretAccessKey == null) {
            return new InstanceProfileCredentialsProvider();
        }
        return new StaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }

    public Region getAWSRegion() {
        return Region.getRegion(region == null ? Regions.DEFAULT_REGION : Regions.fromName(region));
    }
}