/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.slice.Murmur3Hash32;

import java.security.SecureRandom;
import java.util.Arrays;

public class Test
{
//    @org.testng.annotations.Test
    public void testName()
            throws Exception
    {
        SecureRandom numberGenerator = new SecureRandom();

        int size = 2 << 24;
        long[] mosts = new long[size];
        long[] leasts = new long[size];

        int i = 0;
        int collision = 0;
        while (true) {
            long least = numberGenerator.nextLong();
            long most = numberGenerator.nextLong();

            int mostIndex = Murmur3Hash32.hash(most) % size;
            int leastIndex = Murmur3Hash32.hash(least) % size;

            if (mosts[mostIndex] != 0 && leasts[leastIndex] != 0) {
                collision++;
            }

            mosts[mostIndex] = most;
            leasts[leastIndex] = least;

            if (i++ % 100000 == 0) {
                System.out.printf("collision: %d", collision);
                System.out.printf(" m: %.2f", Arrays.stream(mosts).filter(e -> e == 0).count() / (double) size);
                System.out.printf(" l: %.2f", Arrays.stream(leasts).filter(e -> e == 0).count() / (double) size);
                System.out.println(" -> " + i);
            }
        }
    }
}
