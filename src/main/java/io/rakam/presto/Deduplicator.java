/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Deduplicator
{
    private final static int TTL = Ints.checkedCast(ChronoUnit.MONTHS.getDuration().getSeconds());

    private final static byte[] EMPTY_ARRAY = new byte[0];

    private final TtlDB db;
    private final File file;
    private final ReadOptions readOptions;
    private WriteOptions writeOpts;

    public Deduplicator(File file)
            throws RocksDBException
    {
        RocksDB.loadLibrary();
        writeOpts = new WriteOptions();
        this.file = file;

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
            );

            final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            try (final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
                this.db = TtlDB.open(options, this.file.getAbsolutePath(), cfDescriptors, columnFamilyHandleList, ImmutableList.of(TTL), false);
            }

            readOptions = new ReadOptions().setIgnoreRangeDeletions(true).setVerifyChecksums(false);
        }
    }

    public boolean contains(byte[] key)
    {
        try {
            return db.get(readOptions, key) != null;
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(byte[] key)
    {
        try {
            db.put(writeOpts, key, EMPTY_ARRAY);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public long count()
    {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
