package io.rakam.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.rakam.presto.kafka.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.rakam.util.JsonHelper;

import java.time.Instant;
import java.util.Random;

import static io.rakam.presto.kafka.KafkaUtil.createProducerConfig;

public class StressProducer
{
    public static void main(String[] args)
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(createProducerConfig(new KafkaConfig().setNodes("127.0.0.1:9092").setTopic("stress"), null));

        long now = Instant.now().toEpochMilli();
        Random random1 = new Random();
        int i = 0;
        while (true) {
            int random = random1.nextInt(1000 * 60);

            long value = now - random;

            if (i % 100 == 0) {
                value -= random1.nextInt(1000 * 60 * 60 * 24 * 60);
            }

            i++;
            producer.send(new ProducerRecord<>("stress", JsonHelper.encodeAsBytes(ImmutableMap.of(
                    "id", "test",
                    "metadata", ImmutableMap.builder().build(),
                    "data", ImmutableMap.builder()
                            .put("_project", "tweet_stress")
                            .put("_collection", "tweet" + (i % 200))
                            .put("place", "USA")
                            .put("id", 34235435 * i)
                            .put("place_id", 45 * i)
                            .put("place_type", "tefdsfsdfts" + i)
                            .put("user_lang", "fdsfsdfen" + i)
                            .put("has_media", false)
                            .put("_time", value)
                            .put("user_mentions", ImmutableList.of("test", "test3", "test" + i))
                            .put("is_retweet", true)
                            .put("country_code", "USA" + i)
                            .put("user_followers", 445 + i)
                            .put("language", "ENGLISH" + i)
                            .put("user_status_count", 3434 + i)
                            .put("_user", "4353454534534534534trgd" + i)
                            .put("user_created", 432342 + i)
                            .put("longitude", 432342 + i)
                            .put("is_reply", false)
                            .put("latitude", 432342 + i)
                            .put("is_positive", false).build()))));

            if (i % 10000 == 0) {
                System.out.println(i);
            }
        }
    }
}
