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
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public abstract class PageReader<T>
{
    private final PageReaderDeserializer<T> datumReader;
    private final String checkpointColumn;
    private List<ColumnMetadata> expectedSchema;
    private List<ColumnMetadata> actualSchema;
    private PageBuilder pageBuilder;

    public PageReader(String checkpointColumn, List<ColumnMetadata> actualSchema, List<ColumnMetadata> expectedSchema)
    {
        List<Type> prestoSchema = expectedSchema.stream()
                .filter(a -> !a.getName().startsWith("$") && !a.getName().equals(checkpointColumn))
                .map(field -> field.getType())
                .collect(Collectors.toList());

        this.checkpointColumn = checkpointColumn;
        this.expectedSchema = expectedSchema;
        this.actualSchema = actualSchema;
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

    public Page getPage()
    {
        int shardTimeIdx = -1;
        for (int i = 0; i < expectedSchema.size(); i++) {
            if (expectedSchema.get(i).getName().equals(checkpointColumn)) {
                shardTimeIdx = i;
                break;
            }
        }

        Page build = pageBuilder.build();

        if(shardTimeIdx > -1) {
            int channelCount = build.getChannelCount() + 1;

            Block[] blocks = new Block[channelCount];
            Block[] oldBlocks = build.getBlocks();
            System.arraycopy(oldBlocks, 0, blocks, 0, shardTimeIdx);
            System.arraycopy(oldBlocks, shardTimeIdx, blocks, shardTimeIdx + 1, build.getChannelCount() - shardTimeIdx);

            blocks[shardTimeIdx] = RunLengthEncodedBlock.create(TIMESTAMP, null, build.getPositionCount());

            return new Page(blocks);
        }

        return build;
    }

    public List<ColumnMetadata> getActualSchema()
    {
        return actualSchema;
    }

    public void read(T decoder)
            throws IOException
    {
        datumReader.read(decoder);
    }

    public List<ColumnMetadata> getExpectedSchema()
    {
        return expectedSchema;
    }

    public void setActualSchema(List<ColumnMetadata> actualSchema)
    {
        this.actualSchema = actualSchema;
        this.expectedSchema = actualSchema;
    }
}
