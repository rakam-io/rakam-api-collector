/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.connector.MetadataDao;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class S3DatabaseHandler
        implements DatabaseHandler {
    private static final Logger log = Logger.get(S3DatabaseHandler.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).withLocale(Locale.ENGLISH).withZone(ZoneOffset.UTC);

    private final ScheduledExecutorService scheduler;
    private final AmazonS3 s3Client;
    private final S3TargetConfig config;
    private final FieldNameConfig fieldNameConfig;
    private final MetadataDao dao;
    private final MemoryTracker memoryTracker;

    private Map<String, Queue<CollectionBatch>> collectionsBuffer;
    // TODO: clean the buffer periodically
    private final Map<String, DynamicSliceOutput> mainBuffer;

    @Inject
    public S3DatabaseHandler(S3TargetConfig config, MemoryTracker memoryTracker, @Named("metadata.store.jdbc") JDBCPoolDataSource prestoMetastoreDataSource, TypeManager typeManager, FieldNameConfig fieldNameConfig) {
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("s3-writer").setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error(e, "S3 uploader thread is halted");
            }
        }).build());
        collectionsBuffer = new ConcurrentHashMap<>();
        mainBuffer = new ConcurrentHashMap();
        this.memoryTracker = memoryTracker;
        this.config = config;
        this.fieldNameConfig = fieldNameConfig;
        AmazonS3ClientBuilder builder = AmazonS3Client.builder().withCredentials(config.getCredentials());

        if (config.getEndpoint() != null) {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.getEndpoint(), null));
        }

        if (config.getRegion() != null) {
            builder.withRegion(config.getRegion());
        }

        DBI dbi = new DBI(prestoMetastoreDataSource);
        dbi.registerMapper(new MetadataDao.TableColumn.Mapper(typeManager));
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.s3Client = builder.build();
    }

    @PostConstruct
    public void schedule() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long existingBufferSize = mainBuffer.values().stream().mapToLong(value -> value.getRetainedSize()).sum();
                for (Map.Entry<String, Queue<CollectionBatch>> entry : collectionsBuffer.entrySet()) {
                    String project = entry.getKey();
                    Queue<CollectionBatch> batches = entry.getValue();
                    if (batches.isEmpty()) {
                        continue;
                    }

                    DynamicSliceOutput buffer = mainBuffer.computeIfAbsent(project, p -> new DynamicSliceOutput(100000));

                    String fileName = String.format("%s/%s.json.gzip", project, UUID.randomUUID().toString());

                    ArrayList<CompletableFuture> futures = new ArrayList();
                    long maxDataSizeInBytes = config.getMaxDataSize().toBytes();

                    GZIPOutputStream out = new GZIPOutputStream(buffer);

                    while (!batches.isEmpty() && buffer.getRetainedSize() < maxDataSizeInBytes) {
                        CollectionBatch collectionBatch = batches.poll();
                        out.write((byte[]) collectionBatch.buffer.slice().getBase(), 0, collectionBatch.buffer.size());
                        futures.add(collectionBatch.future);
                    }

                    out.finish();
                    out.close();

                    ObjectMetadata objectMetadata = new ObjectMetadata();
                    objectMetadata.setContentLength(buffer.size());
                    PutObjectRequest putObjectRequest = new PutObjectRequest(config.getS3Bucket(),
                            fileName,
                            new SafeSliceInputStream(new BasicSliceInput(buffer.slice())),
                            objectMetadata);

                    tryPutFile(putObjectRequest, 5);

                    futures.forEach(future -> future.complete(null));

                    buffer.reset();
                }
                long finalBufferSize = mainBuffer.values().stream().mapToLong(value -> value.getRetainedSize()).sum();
                memoryTracker.reserveMemory(finalBufferSize - existingBufferSize);
            } catch (Exception e) {
                log.error(e, "Error sending file to S3");
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public List<ColumnMetadata> getColumns(String schema, String table) {
        List<ColumnMetadata> tableColumns = dao.listTableColumns(schema, table).stream()
                .map(e -> new ColumnMetadata(e.getColumnName(), e.getDataType())).collect(Collectors.toList());
        if (tableColumns.isEmpty()) {
            throw new IllegalArgumentException("Table doesn't exist");
        }

        return tableColumns;
    }


    @Override
    public List<ColumnMetadata> addColumns(String schema, String table, List<ColumnMetadata> columns) {
        throw new IllegalStateException("S3 adapter does not support changing the schema");
    }

    @Override
    public Inserter insert(String schema, String table) {
        return new S3Inserter(new SchemaTableName(schema, table), getColumns(schema, table));
    }

    public class S3Inserter implements Inserter {
        private final SchemaTableName table;
        private final int userColumnIndex;
        private final int timeColumnIndex;
        private final List<ColumnMetadata> columns;
        private final DynamicSliceOutput output;
        private final JsonGenerator generator;

        private int findColumnIndex(String fieldName) {
            return IntStream.range(0, columns.size()).filter(e -> columns.get(e).getName().equals(fieldName)).findAny().getAsInt();
        }

        public S3Inserter(SchemaTableName table, List<ColumnMetadata> columns) {
            this.output = new DynamicSliceOutput(10000);
            this.table = table;
            this.columns = columns;
            userColumnIndex = findColumnIndex(fieldNameConfig.getUserFieldName());
            timeColumnIndex = findColumnIndex(fieldNameConfig.getTimeField());
            JsonFactory factory = JsonHelper.getMapper().getFactory();
            try {
                generator = factory.createGenerator((DataOutput) output);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void addPage(Page page) {
            Block[] blocks = page.getBlocks();
            Block userBlock = blocks[userColumnIndex];
            Block timeBlock = blocks[timeColumnIndex];

            try {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    String user = VARCHAR.getSlice(userBlock, i).toString(StandardCharsets.UTF_8);
                    long timestampInMillis = TIMESTAMP.getLong(timeBlock, i);

                    generator.writeStartObject();

                    generator.writeFieldName(fieldNameConfig.getTimeField());
                    generator.writeNumber(timestampInMillis);

                    generator.writeFieldName("$schema");
                    generator.writeString(table.getSchemaName());

                    generator.writeFieldName("$table");
                    generator.writeString(table.getTableName());

                    generator.writeFieldName(fieldNameConfig.getUserFieldName());
                    generator.writeString(user);

                    generator.writeFieldName("properties");

                    // PROPS START
                    generator.writeStartObject();
                    for (int colIdx = 0; colIdx < blocks.length; colIdx++) {
                        if (colIdx == userColumnIndex || colIdx == timeColumnIndex) {
                            continue;
                        }

                        Block block = blocks[colIdx];
                        ColumnMetadata columnMetadata = columns.get(colIdx);
                        Type type = columnMetadata.getType();

                        if (block.isNull(i)) {
                            continue;
                        }

                        generator.writeFieldName(columnMetadata.getName());
                        writeValue(type, generator, block, i);
                    }
                    generator.writeEndObject();
                    // PROPS END

                    generator.writeEndObject();
                    generator.writeRaw('\n');
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void writeValue(Type type, JsonGenerator generator, Block block, int colIdx)
                throws IOException {
            if (block.isNull(colIdx)) {
                generator.writeNull();
                return;
            }

            if (type.equals(DOUBLE)) {
                generator.writeNumber(DOUBLE.getDouble(block, colIdx));
            } else if (type.equals(BIGINT)) {
                generator.writeNumber(BIGINT.getLong(block, colIdx));
            } else if (type.equals(BOOLEAN)) {
                generator.writeBoolean(BOOLEAN.getBoolean(block, colIdx));
            } else if (type.equals(VARCHAR)) {
                generator.writeString(VARCHAR.getSlice(block, colIdx).toString(StandardCharsets.UTF_8));
            } else if (type.equals(INTEGER)) {
                generator.writeNumber(INTEGER.getLong(block, colIdx));
            } else if (type.equals(DATE)) {
                generator.writeString(LocalDate.ofEpochDay(DATE.getLong(block, colIdx)).format(DateTimeFormatter.BASIC_ISO_DATE));
            } else if (type.equals(TIMESTAMP)) {
                generator.writeString(FORMATTER.format(Instant.ofEpochMilli(TimestampType.TIMESTAMP.getLong(block, colIdx))));
            } else {
                if (type instanceof ArrayType) {
                    generator.writeStartArray();

                    if (block.isNull(colIdx)) {
                        generator.writeNull();
                    } else {
                        Type elementType = ((ArrayType) type).getElementType();

                        Block object = block.getObject(colIdx, Block.class);
                        for (int i1 = 0; i1 < object.getPositionCount(); i1++) {
                            writeValue(elementType, generator, object, i1);
                        }
                    }

                    generator.writeEndArray();
                } else if (type instanceof MapType) {
                    MapBlock mapBlock = (MapBlock) block;

                    generator.writeStartObject();

                    Set<String> uniqueKeys = new HashSet<>();

                    SingleMapBlock object = (SingleMapBlock) mapBlock.getObject(colIdx, Block.class);
                    Type elementType = ((MapType) type).getValueType();

                    for (int i = 0; i < object.getPositionCount(); i += 2) {
                        String fieldName = VARCHAR.getSlice(object, i).toStringUtf8();
                        if (!uniqueKeys.contains(fieldName)) {
                            generator.writeFieldName(fieldName);
                            writeValue(elementType, generator, object, i + 1);
                            uniqueKeys.add(fieldName);
                        }
                    }


                    generator.writeEndObject();
                } else {
                    throw new IllegalStateException("Unknown type");
                }
            }
        }

        @Override
        public CompletableFuture<Void> commit() {
            CompletableFuture future = new CompletableFuture();
            try {
                generator.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            collectionsBuffer.computeIfAbsent(table.getSchemaName(), schema -> new ConcurrentLinkedQueue())
                    .add(new CollectionBatch(output, future));

            return future;
        }
    }

    private void tryPutFile(PutObjectRequest putObjectRequest, int numberOfTry) {
        try {
            s3Client.putObject(putObjectRequest);
        } catch (SdkClientException e) {
            if (numberOfTry == 0) {
                log.error(e);
                throw e;
            } else {
                tryPutFile(putObjectRequest, numberOfTry - 1);
            }
        }
    }

    private static class SafeSliceInputStream
            extends InputStream {
        private final BasicSliceInput sliceInput;

        public SafeSliceInputStream(BasicSliceInput sliceInput) {
            this.sliceInput = sliceInput;
        }

        @Override
        public int read() {
            return sliceInput.read();
        }

        @Override
        public int read(byte[] b) {
            return sliceInput.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return sliceInput.read(b, off, len);
        }

        @Override
        public long skip(long n) {
            return sliceInput.skip(n);
        }

        @Override
        public int available() {
            return sliceInput.available();
        }

        @Override
        public void close() {
            sliceInput.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            throw new RuntimeException("mark/reset not supported");
        }

        @Override
        public synchronized void reset()
                throws IOException {
            throw new IOException("mark/reset not supported");
        }

        @Override
        public boolean markSupported() {
            return false;
        }
    }

    public static class CollectionBatch {
        public final DynamicSliceOutput buffer;
        public final CompletableFuture future;


        public CollectionBatch(DynamicSliceOutput buffer, CompletableFuture future) {
            this.buffer = buffer;
            this.future = future;
        }
    }
}
