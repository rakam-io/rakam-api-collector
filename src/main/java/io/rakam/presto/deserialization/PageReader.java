/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public abstract class PageReader<T>
{
    protected final String checkpointColumn;
    private final PageReaderDeserializer<T> datumReader;
    private List<ColumnMetadata> expectedSchema;
    private List<ColumnMetadata> actualSchema;
    private PageBuilder pageBuilder;

    public PageReader(String checkpointColumn, List<ColumnMetadata> schema)
    {
        List<ColumnMetadata> expectedSchema = schema.stream()
                .filter(a -> !a.getName().equals(checkpointColumn))
                .collect(Collectors.toList());

        List<Type> prestoSchema = expectedSchema.stream().map(field -> field.getType()).collect(Collectors.toList());

        this.checkpointColumn = checkpointColumn;
        this.expectedSchema = expectedSchema;
        this.actualSchema = schema;
        this.pageBuilder = new PageBuilder(prestoSchema);
        this.datumReader = createReader();
    }

    public PageBuilder getPageBuilder()
    {
        return pageBuilder;
    }

    public void setPageBuilder(PageBuilder pageBuilder)
    {
        this.pageBuilder = pageBuilder;
    }

    public abstract PageReaderDeserializer<T> createReader();

    public Page buildPage()
    {
        int shardTimeIdx = -1;
        for (int i = 0; i < actualSchema.size(); i++) {
            if (actualSchema.get(i).getName().equals(checkpointColumn)) {
                shardTimeIdx = i;
                break;
            }
        }

        Page build = pageBuilder.build();

        if (shardTimeIdx > -1) {
            int channelCount = build.getChannelCount() + 1;

            Block[] blocks = new Block[channelCount];
            Block[] oldBlocks = build.getBlocks();
            System.arraycopy(oldBlocks, 0, blocks, 0, shardTimeIdx);
            System.arraycopy(oldBlocks, shardTimeIdx, blocks, shardTimeIdx + 1, build.getChannelCount() - shardTimeIdx);

            blocks[shardTimeIdx] = RunLengthEncodedBlock.create(TIMESTAMP, Instant.now().toEpochMilli(), build.getPositionCount());

            return new Page(blocks);
        }

        return build;
    }

    public List<ColumnMetadata> getActualSchema()
    {
        return actualSchema;
    }

    public void setActualSchema(List<ColumnMetadata> actualSchema)
    {
        this.actualSchema = actualSchema;

        this.expectedSchema = actualSchema.stream()
                .filter(a -> !a.getName().startsWith("$") && !a.getName().equals(checkpointColumn))
                .collect(Collectors.toList());
    }

    public void read(T decoder)
            throws IOException
    {
        datumReader.read(decoder, null);
    }

    public void read(T decoder, List<ColumnMetadata> expectedSchema)
            throws IOException
    {
        datumReader.read(decoder, expectedSchema);
    }

    public List<ColumnMetadata> getExpectedSchema()
    {
        return expectedSchema;
    }
}
