/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.rmi.dgc.VMID;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel.NONE;
import static com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel.SUMMARY;

public class KinesisWorkerManager {
    private final KinesisStreamSourceConfig config;
    private final List<Thread> threads;
    private final IRecordProcessorFactory recordProcessorFactory;

    @Inject
    public KinesisWorkerManager(KinesisStreamSourceConfig config, IRecordProcessorFactory recordProcessorFactory) {
        this.config = config;
        this.threads = new ArrayList<>();
        this.recordProcessorFactory = recordProcessorFactory;
    }

    @PostConstruct
    public void initializeWorker() {
        Thread middlewareWorker = createMiddlewareWorker();
        middlewareWorker.start();
        threads.add(middlewareWorker);
    }

    private Worker getWorker(IRecordProcessorFactory factory, KinesisClientLibConfiguration config) {
        AmazonDynamoDBClientBuilder dynamodbBuilder = AmazonDynamoDBClient.builder().withCredentials(config.getDynamoDBCredentialsProvider());
        dynamodbBuilder.withClientConfiguration(config.getDynamoDBClientConfiguration());
        if (this.config.getDynamodbEndpoint() != null) {
            dynamodbBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(this.config.getDynamodbEndpoint(), null));
        }

        AmazonKinesisClientBuilder kinesisBuilder = AmazonKinesisClient.builder().withCredentials(config.getKinesisCredentialsProvider());
        kinesisBuilder.withClientConfiguration(config.getKinesisClientConfiguration());

        config.withMetricsLevel(this.config.getEnableCloudWatch() ? SUMMARY : NONE);

        AmazonCloudWatchClientBuilder cwBuilder = AmazonCloudWatchClient.builder().withCredentials(config.getCloudWatchCredentialsProvider())
                .withClientConfiguration(config.getCloudWatchClientConfiguration());

        return new Worker.Builder().config(config)
                .recordProcessorFactory(factory)
                .kinesisClient(kinesisBuilder.build())
                .dynamoDBClient(dynamodbBuilder.build())
                .cloudWatchClient(cwBuilder.build())
                .build();
    }

    private Thread createMiddlewareWorker() {
        KinesisClientLibConfiguration configuration = new KinesisClientLibConfiguration(config.getDynamodbTable(),
                this.config.getStreamName(),
                this.config.getCredentials(),
                new VMID().toString())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withUserAgent("rakam-middleware-consumer")
                .withFailoverTimeMillis(30000)
                .withCallProcessRecordsEvenForEmptyRecordList(true);

        if (config.getMaxKinesisRecordsPerBatch() != null) {
            configuration.withMaxRecords(config.getMaxKinesisRecordsPerBatch());
        }
        if (this.config.getKinesisEndpoint() == null && this.config.getDynamodbEndpoint() == null) {
            configuration.withRegionName(this.config.getRegion());
        }

        if (config.getKinesisEndpoint() != null) {
            configuration.withKinesisEndpoint(config.getKinesisEndpoint());
        }

        Thread middlewareWorkerThread;
        try {
            Worker worker = getWorker(recordProcessorFactory, configuration);
            middlewareWorkerThread = new Thread(worker);
        } catch (Exception e) {
            throw new RuntimeException("Error creating Kinesis stream worker", e);
        }

        middlewareWorkerThread.setName("middleware-consumer-thread");
        return middlewareWorkerThread;
    }

    @PreDestroy
    public void destroyWorkers() {
        threads.forEach(Thread::interrupt);
    }
}
