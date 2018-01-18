/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import io.rakam.presto.BasicMemoryBuffer;

public class KinesisRecordSizeCalculator
        implements BasicMemoryBuffer.SizeCalculator<Record>
{
    @Override
    public long calculate(Record record)
    {
        return record.getData().remaining();
    }
}
