/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class TestDeduplicator
{
    @Test
    public void testName()
            throws Exception
    {
        Deduplicator deduplicator = new Deduplicator(new File("/tmp/rocksdb"));

        for (int i = 0; i < 10000; i++) {
            deduplicator.put(Long.toString(i).getBytes(StandardCharsets.UTF_8));
        }

        long l;

        l = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            deduplicator.put(Long.toString(i).getBytes(StandardCharsets.UTF_8));
        }
        System.out.println(System.currentTimeMillis() - l);

        l = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            deduplicator.contains(Long.toString(i).getBytes(StandardCharsets.UTF_8));
        }
        System.out.println(System.currentTimeMillis() - l);
    }
}
