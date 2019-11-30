/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage.backup;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.facebook.presto.rakam.S3BackupConfig;
import com.facebook.presto.raptor.RaptorErrorCode;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.storage.InMemoryBuffer;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.rakam.presto.MemoryTracker;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;

public class S3BackupStore
        implements BackupStore
{
    private static final int TRY_COUNT = 5;
    private final AmazonS3 s3Client;
    private final S3BackupConfig config;
    private final InMemoryBuffer buffer;
    private final MemoryTracker memoryTracker;

    @Inject
    public S3BackupStore(S3BackupConfig config, MemoryTracker memoryTracker, InMemoryBuffer buffer)
    {
        this.config = config;
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(config.getCredentials());

        if (config.getEndpoint() != null) {
            amazonS3ClientBuilder.setEndpointConfiguration(new EndpointConfiguration(config.getEndpoint(), config.getAWSRegion().getName()));
            amazonS3ClientBuilder.disableChunkedEncoding().enablePathStyleAccess();
        }
        else {
            amazonS3ClientBuilder.setRegion(Regions.fromName(config.getAWSRegion().getName()).getName());
        }

        s3Client = amazonS3ClientBuilder.build();
        this.buffer = buffer;
        this.memoryTracker = memoryTracker;
    }

    public void backupShard(UUID uuid, File source)
    {
        DynamicSliceOutput sliceOutput = buffer.remove(source.getName());

        try {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            Slice slice = sliceOutput.slice();
            md5.update((byte[]) slice.getBase(), 0, slice.length());

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(slice.length());
            objectMetadata.setContentMD5(Base64.getEncoder().encodeToString(md5.digest()));

            try {
                tryUpload(slice, uuid.toString(), objectMetadata, TRY_COUNT);
            }
            catch (Exception ex) {
                throw new PrestoException(RaptorErrorCode.RAPTOR_BACKUP_ERROR, "Failed to create backup shard file on S3", ex);
            }
        }
        finally {
            memoryTracker.freeMemory(sliceOutput.getRetainedSize());
        }
    }

    private void tryUpload(Slice slice, String key, ObjectMetadata objectMetadata, int tryCount)
    {
        try {
            SafeSliceInputStream input = new SafeSliceInputStream(slice.getInput());
            s3Client.putObject(config.getS3Bucket(), key, input, objectMetadata);
            input.close();
        }
        catch (Exception ex) {
            if (tryCount > 0) {
                tryUpload(slice, key, objectMetadata, tryCount - 1);
            }
            else {
                throw new PrestoException(RaptorErrorCode.RAPTOR_BACKUP_ERROR, "Failed to create backup shard file on S3", ex);
            }
        }
    }

    public void restoreShard(UUID uuid, File target)
    {
        // no-op
    }

    public boolean deleteShard(UUID uuid)
    {
        throw new UnsupportedOperationException();
    }

    public boolean shardExists(UUID uuid)
    {
        try {
            this.s3Client.getObjectMetadata(this.config.getS3Bucket(), uuid.toString());
            return true;
        }
        catch (AmazonS3Exception var3) {
            if (var3.getStatusCode() == 404) {
                return false;
            }
            else {
                throw var3;
            }
        }
    }

    private static class SafeSliceInputStream
            extends InputStream
    {
        private final BasicSliceInput sliceInput;

        public SafeSliceInputStream(BasicSliceInput sliceInput)
        {
            this.sliceInput = sliceInput;
        }

        public int read()
        {
            return this.sliceInput.read();
        }

        public int read(byte[] b)
        {
            return this.sliceInput.read(b);
        }

        public int read(byte[] b, int off, int len)
        {
            return this.sliceInput.read(b, off, len);
        }

        public long skip(long n)
        {
            return this.sliceInput.skip(n);
        }

        public int available()
        {
            return this.sliceInput.available();
        }

        public void close()
        {
            this.sliceInput.close();
        }

        public synchronized void mark(int readlimit)
        {
            throw new RuntimeException("mark/reset not supported");
        }

        public synchronized void reset()
                throws IOException
        {
            throw new IOException("mark/reset not supported");
        }

        public boolean markSupported()
        {
            return false;
        }
    }
}