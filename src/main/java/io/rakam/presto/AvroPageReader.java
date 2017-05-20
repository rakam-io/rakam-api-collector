/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.PageDatumReader;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static io.rakam.presto.AvroUtil.convertAvroSchema;

public class AvroPageReader
{
    private final PageDatumReader datumReader;
    private final List<ColumnMetadata> expectedSchema;
    private PageBuilder pageBuilder;

    public AvroPageReader(List<ColumnMetadata> rakamSchema)
    {
        this(rakamSchema, rakamSchema);
    }

    public AvroPageReader(List<ColumnMetadata> actualSchema, List<ColumnMetadata> expectedSchema)
    {
        List<Type> prestoSchema = expectedSchema.stream()
                .filter(a -> !a.getName().startsWith("$") && !a.getName().equals("_shard_time"))
                .map(field -> field.getType())
                .collect(Collectors.toList());

        this.expectedSchema = expectedSchema;
        this.pageBuilder = new PageBuilder(prestoSchema);
        this.datumReader = new PageDatumReader(pageBuilder,
                convertAvroSchema(actualSchema),
                convertAvroSchema(expectedSchema));
    }

    public Page getPage()
    {
        int shardTimeIdx = -1;
        for (int i = 0; i < expectedSchema.size(); i++) {
            if (expectedSchema.get(i).getName().equals("_shard_time")) {
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

    public void read(BinaryDecoder decoder)
            throws IOException
    {
        datumReader.read(null, decoder);
    }

    public List<ColumnMetadata> getExpectedSchema()
    {
        return expectedSchema;
    }

    public void setActualSchema(List<ColumnMetadata> actualSchema)
    {
        datumReader.setSchema(convertAvroSchema(actualSchema));
    }
}
