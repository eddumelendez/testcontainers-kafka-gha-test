package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class KafkaContainerTest {

    @ParameterizedTest(name = "kafka image version {0}")
    @ValueSource(strings = {"6.2.2", "7.0.1"})
    void testUsage(String tag) throws Exception {
        DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:" + tag);

        try (KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)
                .withNetwork(null)
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        ) {
            kafka.start();
            testKafkaFunctionality(kafka.getBootstrapServers(), 1, 1);
        }
    }

    protected void testKafkaFunctionality(String bootstrapServers, int partitions, int rf) throws Exception {
        try (
                AdminClient adminClient = AdminClient.create(
                        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                );
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        Map.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ProducerConfig.CLIENT_ID_CONFIG,
                                UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        Map.of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG,
                                "tc-" + UUID.randomUUID(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest"
                        ),
                        new StringDeserializer(),
                        new StringDeserializer()
                );
        ) {
            String topicName = "messages-" + UUID.randomUUID();

            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, partitions, (short) rf));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(topicName));

            producer.send(new ProducerRecord<>(topicName, "testcontainers", "rulezzz")).get();

            Unreliables.retryUntilTrue(
                    10,
                    TimeUnit.SECONDS,
                    () -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                        if (records.isEmpty()) {
                            return false;
                        }

                        assertThat(records)
                                .hasSize(1)
                                .extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
                                .containsExactly(tuple(topicName, "testcontainers", "rulezzz"));

                        return true;
                    }
            );

            consumer.unsubscribe();
        }
    }

}
