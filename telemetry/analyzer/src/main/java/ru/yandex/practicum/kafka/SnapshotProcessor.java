package ru.yandex.practicum.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable {
    private final KafkaClient kafkaClient;
    private final SnapshotService snapshotService;
    @Value("${topics.snapshots}")
    private String topic;

    @Override
    public void run() {
        log.info("Старт SnapshotProcessor для топика '{}'", topic);
        Consumer<String, SensorsSnapshotAvro> consumer = kafkaClient.getConsumerSnapshot();
        consumer.subscribe(List.of(topic));
        try {
            while (true) {
                log.debug("Ожидание сообщений Kafka...");
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro snapshot = record.value();
                    log.info("Получен снапшот устройств хаба {} с {} датчиками", snapshot.getHubId(), snapshot.getSensorsState().size());
                    snapshotService.processSnapshot(snapshot);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка обработки сообщений", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception ignored) {
            }
            consumer.close();
            log.info("Консьюмер закрыт");
        }
    }
}
