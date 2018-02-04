/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import io.airlift.slice.SizeOf;
import io.rakam.presto.BasicMemoryBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.openjdk.jol.info.ClassLayout;

public class KafkaRecordSizeCalculator
        implements BasicMemoryBuffer.SizeCalculator<ConsumerRecord<byte[], byte[]>>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConsumerRecord.class).instanceSize();
    private static final int TOTAL = INSTANCE_SIZE + (SizeOf.SIZE_OF_LONG * 3) + SizeOf.SIZE_OF_INT;

    @Override
    public long calculate(ConsumerRecord<byte[], byte[]> record)
    {
        byte[] key = record.key();
        int keyLength = key == null ? 0 : key.length;
        return keyLength + record.value().length + TOTAL + record.topic().length();
    }
}
