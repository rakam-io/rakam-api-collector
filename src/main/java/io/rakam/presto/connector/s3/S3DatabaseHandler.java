/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.connector.raptor.RaptorConfig;
import io.rakam.presto.connector.raptor.RaptorDatabaseHandler;
import io.rakam.presto.connector.raptor.S3BackupConfig;
import org.rakam.util.JsonHelper;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class S3DatabaseHandler
        extends RaptorDatabaseHandler
{
    private static final Logger log = Logger.get(S3DatabaseHandler.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).withLocale(Locale.ENGLISH).withZone(ZoneOffset.UTC);

    private final S3TargetConfig config;
    private final FieldNameConfig fieldNameConfig;
    private final ThreadPoolExecutor s3ThreadPool;
    Map<SchemaTableName, List<ColumnMetadata>> schemaCache;
    private final AmazonS3Client s3Client;

    @Inject
    public S3DatabaseHandler(S3TargetConfig config, RaptorConfig raptorConfig, TypeManager typeManager, S3BackupConfig s3BackupConfig, FieldNameConfig fieldNameConfig, MemoryTracker memoryTracker)
    {
        super(raptorConfig, typeManager, s3BackupConfig, fieldNameConfig, memoryTracker);
        s3ThreadPool = new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        schemaCache = new HashMap<>();
        this.config = config;
        this.fieldNameConfig = fieldNameConfig;
        s3Client = new AmazonS3Client(config.getCredentials());
        s3Client.setRegion(config.getAWSRegion());
        if (config.getEndpoint() != null) {
            s3Client.setEndpoint(config.getEndpoint());
        }
    }

    @Override
    public List<ColumnMetadata> getColumns(String schema, String table)
    {
        List<ColumnMetadata> columns = super.getColumns(schema, table);
        schemaCache.put(new SchemaTableName(schema, table), columns);
        return columns;
    }

    @Override
    public Inserter insert(String schema, String table)
    {
        return new S3Inserter(new SchemaTableName(schema, table));
    }

    public class S3Inserter
            implements Inserter
    {
        private final SchemaTableName table;
        private final DynamicSliceOutput output;
        private final int userColumnIndex;
        private final int timeColumnIndex;
        private final List<ColumnMetadata> columns;
        private final JsonGenerator generator;

        public S3Inserter(SchemaTableName table)
        {
            this.table = table;
            output = new DynamicSliceOutput(10000);
            this.columns = schemaCache.get(table);
            userColumnIndex = IntStream.range(0, columns.size()).filter(e -> columns.get(e).getName().equals(fieldNameConfig.getUserFieldName())).findAny().getAsInt();
            timeColumnIndex = IntStream.range(0, columns.size()).filter(e -> columns.get(e).getName().equals(fieldNameConfig.getTimeField())).findAny().getAsInt();

            JsonFactory factory = JsonHelper.getMapper().getFactory();
            try {
                generator = factory.createGenerator((DataOutput) output);
                generator.writeStartArray();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void addPage(Page page)
        {
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

                        generator.writeFieldName(columnMetadata.getName());
                        writeValue(type, generator, block, i);
                    }
                    generator.writeEndObject();
                    // PROPS END

                    generator.writeEndObject();
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void writeValue(Type type, JsonGenerator generator, Block block, int colIdx)
                throws IOException
        {
            if (block.isNull(colIdx)) {
                generator.writeNull();
                return;
            }

            if (type.equals(DOUBLE)) {
                generator.writeNumber(DOUBLE.getDouble(block, colIdx));
            }
            else if (type.equals(BIGINT)) {
                generator.writeNumber(BIGINT.getLong(block, colIdx));
            }
            else if (type.equals(BOOLEAN)) {
                generator.writeBoolean(BOOLEAN.getBoolean(block, colIdx));
            }
            else if (type.equals(VARCHAR)) {
                generator.writeString(VARCHAR.getSlice(block, colIdx).toString(StandardCharsets.UTF_8));
            }
            else if (type.equals(INTEGER)) {
                generator.writeNumber(INTEGER.getLong(block, colIdx));
            }
            else if (type.equals(DATE)) {
                generator.writeString(LocalDate.ofEpochDay(DATE.getLong(block, colIdx)).format(DateTimeFormatter.BASIC_ISO_DATE));
            }
            else if (type.equals(TIMESTAMP)) {
                generator.writeString(FORMATTER.format(Instant.ofEpochMilli(TimestampType.TIMESTAMP.getLong(block, colIdx))));
            }
            else {
                if (type instanceof ArrayType) {
                    Type elementType = ((ArrayType) type).getElementType();

                    generator.writeStartArray();

                    int positionCount = block.getPositionCount();
                    for (int i = 0; i < positionCount; i++) {
                        if (block.isNull(i)) {
                            generator.writeNull();
                        }
                        else {
                            Block object = block.getObject(i, Block.class);
                            for (int i1 = 0; i1 < object.getPositionCount(); i1++) {
                                writeValue(elementType, generator, object, i1);
                            }
                        }
                    }

                    generator.writeEndArray();
                }
                else if (type instanceof MapType) {
                    Type elementType = ((MapType) type).getValueType();
                    MapBlock mapBlock = (MapBlock) block;

                    generator.writeStartObject();

                    Set<String> uniqueKeys = new HashSet<>();

                    int positionCount = block.getPositionCount();
                    for (int i = 0; i < positionCount; i++) {
                        SingleMapBlock object = (SingleMapBlock) mapBlock.getObject(i, Block.class);
                        String fieldName = VARCHAR.getSlice(object, 0).toStringUtf8();
                        if (!uniqueKeys.contains(fieldName)) {
                            generator.writeFieldName(fieldName);
                            writeValue(elementType, generator, object, 1);
                            uniqueKeys.add(fieldName);
                        }
                    }

                    generator.writeEndObject();
                }
                else {
                    throw new IllegalStateException();
                }
            }
        }

        @Override
        public CompletableFuture<Void> commit()
        {
            try {
                generator.writeEndArray();
                generator.flush();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            String fileName = table.getSchemaName() + '/' + table.getTableName() + "|" + UUID.randomUUID().toString() + ".json.gzip";

            DynamicSliceOutput singleOut = new DynamicSliceOutput(output.size() / 2);
            try {
                GZIPOutputStream out = new GZIPOutputStream(singleOut);
                out.write((byte[]) output.slice().getBase(), 0, output.size());
                out.finish();
                out.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(singleOut.size());
            PutObjectRequest putObjectRequest = new PutObjectRequest(config.getS3Bucket(),
                    fileName,
                    new SafeSliceInputStream(new BasicSliceInput(singleOut.slice())),
                    objectMetadata);

            return CompletableFuture.runAsync(() -> {
                tryPutFile(putObjectRequest, 3);
                output.reset();
            }, s3ThreadPool);
        }
    }

    private void tryPutFile(PutObjectRequest putObjectRequest, int numberOfTry)
    {
        try {
            s3Client.putObject(putObjectRequest);
        }
        catch (SdkClientException e) {
            if (numberOfTry == 0) {
                log.error(e);
                throw e;
            }
            else {
                tryPutFile(putObjectRequest, numberOfTry - 1);
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

        @Override
        public int read()
        {
            return sliceInput.read();
        }

        @Override
        public int read(byte[] b)
        {
            return sliceInput.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len)
        {
            return sliceInput.read(b, off, len);
        }

        @Override
        public long skip(long n)
        {
            return sliceInput.skip(n);
        }

        @Override
        public int available()
        {
            return sliceInput.available();
        }

        @Override
        public void close()
        {
            sliceInput.close();
        }

        @Override
        public synchronized void mark(int readlimit)
        {
            throw new RuntimeException("mark/reset not supported");
        }

        @Override
        public synchronized void reset()
                throws IOException
        {
            throw new IOException("mark/reset not supported");
        }

        @Override
        public boolean markSupported()
        {
            return false;
        }
    }
}
