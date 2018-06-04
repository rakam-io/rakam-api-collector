/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.redshift;

import com.amazon.redshift.jdbc42.Driver;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.log.Logger;
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
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class RedshiftDatabaseHandler
        extends RaptorDatabaseHandler
{
    private static final Logger log = Logger.get(RedshiftDatabaseHandler.class);

    private final CompletableFuture<Void> COMPLETED_FUTURE = new CompletableFuture<>();
    private final RedshiftConfig config;
    private final FieldNameConfig fieldNameConfig;
    Map<SchemaTableName, List<ColumnMetadata>> schemaCache;

    @Inject
    public RedshiftDatabaseHandler(RedshiftConfig config, RaptorConfig raptorConfig, TypeManager typeManager, S3BackupConfig s3BackupConfig, FieldNameConfig fieldNameConfig, MemoryTracker memoryTracker)
    {
        super(raptorConfig, typeManager, s3BackupConfig, fieldNameConfig, memoryTracker);
        schemaCache = new ConcurrentHashMap<>();
        this.config = config;
        this.fieldNameConfig = fieldNameConfig;
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
        try {
            return new RedshiftInserter(new SchemaTableName(schema, table));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public class RedshiftInserter
            implements Inserter
    {
        private final SchemaTableName table;
        private final Driver DRIVER = new Driver();
        private final Connection connection;
        private final List<ColumnMetadata> columns;
        private final PreparedStatement statement;
        private final int userColumnIndex;
        private final int timeColumnIndex;
        private final DynamicSliceOutput output;

        public RedshiftInserter(SchemaTableName table)
                throws SQLException
        {
            this.table = table;
            this.connection = DRIVER.connect(config.getUrl(), new Properties());
            this.columns = schemaCache.get(table);
            userColumnIndex = IntStream.range(0, columns.size()).filter(e -> columns.get(e).getName().equals(fieldNameConfig.getUserFieldName())).findAny().getAsInt();
            timeColumnIndex = IntStream.range(0, columns.size()).filter(e -> columns.get(e).getName().equals(fieldNameConfig.getTimeField())).findAny().getAsInt();
            this.statement = this.connection.prepareStatement(format("INSERT INTO rakam_events (_user, collection, _time, properties) values (?, ?, ?, ?)"));
            output = new DynamicSliceOutput(1000);
        }

        @Override
        public void addPage(Page page)
        {
            Block[] blocks = page.getBlocks();
            Block userBlock = blocks[userColumnIndex];
            Block timeBlock = blocks[timeColumnIndex];

            for (int i = 0; i < page.getPositionCount(); i++) {
                try {
                    String user = VARCHAR.getSlice(userBlock, i).toString(StandardCharsets.UTF_8);
                    long timestampInMillis = TIMESTAMP.getLong(timeBlock, i);

                    statement.setString(1, user);
                    statement.setString(2, table.getTableName());
                    statement.setLong(3, timestampInMillis);

                    JsonFactory factory = JsonHelper.getMapper().getFactory();
                    JsonGenerator generator = factory.createGenerator((DataOutput) output);
                    generator.writeStartObject();

                    for (int colIdx = 0; colIdx < blocks.length; colIdx++) {
                        if (colIdx == userColumnIndex || colIdx == timeColumnIndex) {
                            continue;
                        }

                        Block block = blocks[i];
                        ColumnMetadata columnMetadata = columns.get(i);
                        Type type = columnMetadata.getType();

                        generator.writeFieldName(columnMetadata.getName());
                        writeValue(type, generator, block, i);
                    }

                    generator.writeEndObject();

                    statement.setString(4, output.toString(StandardCharsets.UTF_8));
                    output.reset();
                    statement.addBatch();
                }
                catch (SQLException | IOException e) {
                    log.error(e);
                }
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
                generator.writeString(LocalDate.ofEpochDay(DATE.getLong(block, colIdx)).toString());
            }
            else if (type.equals(TIMESTAMP)) {
                generator.writeString(Instant.ofEpochMilli(TimestampType.TIMESTAMP.getLong(block, colIdx)).toString());
            }
            else {
                if (type instanceof ArrayType) {
                    Type elementType = ((ArrayType) type).getElementType();

                    generator.writeStartArray();

                    int positionCount = block.getPositionCount();
                    for (int i = 0; i < positionCount; i++) {
                        writeValue(elementType, generator, block, i);
                    }

                    generator.writeEndArray();
                }

                if (type instanceof MapType) {
                    generator.writeNull();
//                    Type elementType = ((MapType) type).getValueType();
//                    MapBlock mapBlock = (MapBlock) block;
//
//                    generator.writeStartObject();
//
//                    int positionCount = block.getPositionCount();
//                    for (int i = 0; i < positionCount; i++) {
//                        SingleMapBlock object = mapBlock.getObject(i, SingleMapBlock.class);
//
//                        writeValue(elementType, generator, block, i);
//                    }
//
//                    generator.writeEndObject();
                }

                throw new IllegalStateException();
            }
        }

        @Override
        public CompletableFuture<Void> commit()
        {
            try {
                statement.executeBatch();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return COMPLETED_FUTURE;
        }
    }
}
