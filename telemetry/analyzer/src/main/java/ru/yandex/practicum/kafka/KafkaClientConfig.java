package ru.yandex.practicum.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Configuration
public class KafkaClientConfig {

    @Bean
    public KafkaClient getClient() {
        return new KafkaClient() {
            @Value("${kafka.bootstrap-servers}")
            private String bootstrapServers;
            @Value("${kafka.consumer.key-deserializer}")
            private String keyDeserializer;
            @Value("${kafka.consumer.hub-value-deserializer}")
            private String hubValueDeserializer;
            @Value("${kafka.consumer.snapshot-value-deserializer}")
            private String snapshotValueDeserializer;
            @Value("${kafka.consumer.hub-group-id}")
            private String hubIdGroup;
            @Value("${kafka.consumer.snapshot-group-id}")
            private String snapshotIdGroup;


            private Consumer<String, SensorsSnapshotAvro> snpapshotConsumer;
            private Consumer<String, HubEventAvro> hubConsumer;

            @Override
            public Consumer<String, SensorsSnapshotAvro> getConsumerSnapshot() {
                if (snpapshotConsumer == null) {
                    Properties config = new Properties();
                    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
                    config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, snapshotValueDeserializer);
                    config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, snapshotIdGroup);
                    snpapshotConsumer = new KafkaConsumer<>(config);
                }
                return snpapshotConsumer;
            }

            @Override
            public Consumer<String, HubEventAvro> getConsumerHubEvent() {
                if (hubConsumer == null) {
                    Properties config = new Properties();
                    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
                    config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, hubValueDeserializer);
                    config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, hubIdGroup);
                    hubConsumer = new KafkaConsumer<>(config);
                }
                return hubConsumer;
            }

            @Override
            public void stop() {
                if (snpapshotConsumer != null) {
                    snpapshotConsumer.close();
                }
                if (hubConsumer != null) {
                    hubConsumer.close();
                }
            }
        };
    }
}
