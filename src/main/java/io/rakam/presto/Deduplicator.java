/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class Deduplicator
{
    private final static byte[] EMPTY_ARRAY = new byte[0];
    private final static int DELETE_BATCH_SIZE = 100000;
    private final static byte[] SEEK_HINT = "seek_hint".getBytes(StandardCharsets.UTF_8);
    private final static byte[] INDEX_NAME = "index".getBytes(StandardCharsets.UTF_8);
    private final static byte[] SEQUENCE_NUMBER = "sequence_number".getBytes(StandardCharsets.UTF_8);

    private final RocksDB db;
    private final File file;
    private final ColumnFamilyHandle columnFamily;
    private final ReadOptions readOptions;
    private final long maxBytes;
    private int sequenceNumber;
    private WriteOptions writeOpts;

    public Deduplicator(File file)
            throws RocksDBException
    {
        RocksDB.loadLibrary();
        writeOpts = new WriteOptions();
        this.file = file;

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor(INDEX_NAME, cfOpts)
            );

            final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            try (final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
                this.db = RocksDB.open(options, this.file.getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
            }

            columnFamily = columnFamilyHandleList.get(1);
            readOptions = new ReadOptions().setIgnoreRangeDeletions(true).setVerifyChecksums(false);
        }

        maxBytes = DataSize.valueOf("1GB").toBytes();
        sequenceNumber = Optional.ofNullable(db.get(SEQUENCE_NUMBER))
                .map(bytes -> Ints.fromByteArray(bytes)).orElse(0);
    }

    public Map<byte[], byte[]> get(List<byte[]> key)
    {
        try {
            return db.multiGet(readOptions, key);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void put(List<byte[]> key)
    {
        WriteBatch writeBatch = new WriteBatch();
        byte[] sequenceNumberKey = Ints.toByteArray(sequenceNumber);

        for (byte[] value : key) {
            writeBatch.put(columnFamily, sequenceNumberKey, value);
            writeBatch.put(value, EMPTY_ARRAY);
        }

        sequenceNumber += 1;

        writeBatch.put(SEQUENCE_NUMBER, Ints.toByteArray(sequenceNumber));
        try {
            db.write(writeOpts, writeBatch);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void clean()
    {
        try {
            long totalSize = getItemSize() * 16;
            if (totalSize < maxBytes) {
                System.out.println(DataSize.succinctBytes(totalSize));
                return;
            }

            int deleteBatchSize = Math.min(Math.max(Ints.checkedCast((totalSize - maxBytes ) / 32), DELETE_BATCH_SIZE), 10_000_000);

            byte[] value = db.get(SEEK_HINT);
            RocksIterator rocksIterator = db.newIterator(columnFamily);

            if (value != null) {
                rocksIterator.seek(value);
            }
            else {
                rocksIterator.seekToFirst();
            }

            int i = 0;

            WriteBatch writeBatch = new WriteBatch();

            byte[] lastSeq = null;
            while (rocksIterator.isValid() && i < deleteBatchSize) {
                writeBatch.remove(rocksIterator.value());
                lastSeq = rocksIterator.key();
                writeBatch.remove(columnFamily, lastSeq);

                rocksIterator.next();
                i++;
            }


            if (lastSeq == null) {
                return;
            }

            writeBatch.put(SEEK_HINT, lastSeq);

            db.write(writeOpts, writeBatch);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public long getItemSize()
    {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
            throws RocksDBException
    {
        Deduplicator deduplicator = new Deduplicator(new File("/tmp/rocksdb"));
        int batchSize = 20000;

        final AtomicLong total = new AtomicLong();

        Runnable runnable = () -> {
            SecureRandom secureRandom = new SecureRandom();

            byte[][] bytes = new byte[batchSize][];
            List<byte[]> list = Arrays.asList(bytes);

            int i;
            while (true) {
                long start = System.currentTimeMillis();
                i = 0;
                for (int i1 = 0; i1 < batchSize; i1++) {
                    byte[] randomBytes = new byte[16];
                    secureRandom.nextBytes(randomBytes);
                    randomBytes[6] &= 0x0f;  /* clear version        */
                    randomBytes[6] |= 0x40;  /* set to version 4     */
                    randomBytes[8] &= 0x3f;  /* clear variant        */
                    randomBytes[8] |= 0x80;  /* set to IETF variant  */

                    bytes[i++] = randomBytes;
                }

                Map<byte[], byte[]> map = deduplicator.get(list);

                if (map.isEmpty()) {
                    deduplicator.put(list);

                    total.addAndGet(bytes.length);
                }
                else {
                    for (byte[] eventId : bytes) {
                        if (map.containsKey(eventId)) {
                            continue;
                        }

                        total.incrementAndGet();
                        // add
                    }
                }

                System.out.println(total.get() + " -> " + (System.currentTimeMillis() - start));
                deduplicator.clean();
            }
        };

        for (int i = 0; i < 4; i++) {
            Thread thread = new Thread(runnable);
            thread.setName("thread-"+i);
            thread.start();
        }
    }

}
