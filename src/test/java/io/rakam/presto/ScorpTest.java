package io.rakam.presto;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Splitter;

public class ScorpTest {
    public static void main(String[] args) {
        List<String> collections = Splitter.on(",").splitToList(
                "topic_rename,post_spam,client_variables,change_cg,conversation_snap_open,c_u_men,post_unpin,post_create,category_follow," +
                        "post_static_feature,post_send_message_button_tap");
        BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAIODX2QSV7RXBP7CA", "az/hzqWkX+cxl18XgMddRn3QFc5dE6Kpay+sW7xH");
        AmazonS3Client s3Client = new AmazonS3Client(credentials);
        s3Client.setRegion(RegionUtils.getRegion("eu-central-1"));
        AmazonKinesisClient kinesis = new AmazonKinesisClient(credentials);
        kinesis.setRegion(RegionUtils.getRegion("eu-central-1"));

        Instant start = Instant.parse("2017-12-23T05:10:00.00Z");
        Instant end = Instant.parse("2017-12-23T21:40:00.00Z");

        for (String collection : collections) {
            System.out.println(collection);
            ObjectListing listing = s3Client.listObjects("scorp-rakam-bulk", "scorp_data/" + collection);
            List<S3ObjectSummary> summaries = listing.getObjectSummaries();

            int i = summaries.size();
            for (S3ObjectSummary summary : summaries) {
                Instant instant = Instant.ofEpochMilli(summary.getLastModified().getTime());
                if (instant.isAfter(start) && instant.isBefore(end)) {
                    test(kinesis, summary.getKey(), summary.getSize(), 3);
                }
            }

            while (listing.isTruncated()) {
                listing = s3Client.listNextBatchOfObjects(listing);
                summaries = listing.getObjectSummaries();
                for (S3ObjectSummary summary : summaries) {
                    Instant instant = Instant.ofEpochMilli(summary.getLastModified().getTime());
                    if (instant.isAfter(start) && instant.isBefore(end)) {
                        test(kinesis, summary.getKey(), summary.getSize(), 3);
                    }
                }
                i += summaries.size();
                System.out.println(i + " ");
            }
        }
    }

    private static void test(AmazonKinesisClient kinesis, String key, long size, int tryCount) {
        System.out.println(key);
    }

    private static void testReal(AmazonKinesisClient kinesis, String key, long size, int tryCount) {
        String collection = key.split("/", 3)[1];
        ByteBuffer allocate = ByteBuffer.allocate(key.length() + 1 + 8);
        allocate.put((byte) 1);
        allocate.putLong(size);
        allocate.put(key.getBytes(StandardCharsets.UTF_8));
        allocate.clear();

        try {
            kinesis.putRecord("rakam-stream", allocate,
                    "scorp_data" + "|" + collection);
        } catch (Exception e) {
            if (tryCount == 0) {
                throw e;
            }

            test(kinesis, key, size, tryCount - 1);
        }
    }
}
