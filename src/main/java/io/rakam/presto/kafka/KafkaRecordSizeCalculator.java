/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import io.rakam.presto.BasicMemoryBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.openjdk.jol.info.ClassLayout;

public class KafkaRecordSizeCalculator
        implements BasicMemoryBuffer.SizeCalculator<ConsumerRecord<byte[], byte[]>>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConsumerRecord.class).instanceSize();

    @Override
    public long calculate(ConsumerRecord<byte[], byte[]> record)
    {
        return record.serializedKeySize() + record.serializedValueSize() + INSTANCE_SIZE;
    }
}
