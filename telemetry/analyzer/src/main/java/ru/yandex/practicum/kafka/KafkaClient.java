package ru.yandex.practicum.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public interface KafkaClient {

    Consumer<String, SensorsSnapshotAvro> getConsumerSnapshot();

    Consumer<String, HubEventAvro> getConsumerHubEvent();

    void stop();
}
