/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage.backup;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.facebook.presto.rakam.S3BackupConfig;
import com.facebook.presto.raptor.RaptorErrorCode;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.storage.InMemoryFileSystem;
import com.facebook.presto.spi.PrestoException;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;

public class S3BackupStore implements BackupStore {
    private static final Logger log = Logger.get(S3BackupStore.class);

    private final AmazonS3Client s3Client;
    private final S3BackupConfig config;
    private final InMemoryFileSystem inMemoryFileSystem;

    @Inject
    public S3BackupStore(S3BackupConfig config, InMemoryFileSystem inMemoryFileSystem) {
        this.config = config;
        this.s3Client = new AmazonS3Client(config.getCredentials());
        this.s3Client.setRegion(config.getAWSRegion());
        if (config.getEndpoint() != null) {
            this.s3Client.setEndpoint(config.getEndpoint());
        }
        this.inMemoryFileSystem = inMemoryFileSystem;
    }

    public void backupShard(UUID uuid, File source) {
        Slice slice = inMemoryFileSystem.get(source.getName());
        if (slice == null) {
            throw new IllegalStateException();
        }

        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        md5.update((byte[]) slice.getBase(), 0, slice.length());

        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(slice.length());
            objectMetadata.setContentMD5(Base64.getEncoder().encodeToString(md5.digest()));
            SafeSliceInputStream input = new SafeSliceInputStream(slice.getInput());
            this.s3Client.putObject(this.config.getS3Bucket(), uuid.toString(), input, objectMetadata);
            input.close();
        } catch (Exception ex) {
            throw new PrestoException(RaptorErrorCode.RAPTOR_BACKUP_ERROR, "Failed to create backup shard file on S3", ex);
        }

        inMemoryFileSystem.remove(source.getName());
    }

    public void restoreShard(UUID uuid, File target) {
        // no-op
    }

    public boolean deleteShard(UUID uuid) {
        throw new UnsupportedOperationException();
    }

    public boolean shardExists(UUID uuid) {
        try {
            this.s3Client.getObjectMetadata(this.config.getS3Bucket(), uuid.toString());
            return true;
        } catch (AmazonS3Exception var3) {
            if (var3.getStatusCode() == 404) {
                return false;
            } else {
                throw var3;
            }
        }
    }

    private static class SafeSliceInputStream extends InputStream {
        private final BasicSliceInput sliceInput;

        public SafeSliceInputStream(BasicSliceInput sliceInput) {
            this.sliceInput = sliceInput;
        }

        public int read() throws IOException {
            return this.sliceInput.read();
        }

        public int read(byte[] b) throws IOException {
            return this.sliceInput.read(b);
        }

        public int read(byte[] b, int off, int len) throws IOException {
            return this.sliceInput.read(b, off, len);
        }

        public long skip(long n) throws IOException {
            return this.sliceInput.skip(n);
        }

        public int available() throws IOException {
            return this.sliceInput.available();
        }

        public void close() throws IOException {
            this.sliceInput.close();
        }

        public synchronized void mark(int readlimit) {
            throw new RuntimeException("mark/reset not supported");
        }

        public synchronized void reset() throws IOException {
            throw new IOException("mark/reset not supported");
        }

        public boolean markSupported() {
            return false;
        }
    }
}