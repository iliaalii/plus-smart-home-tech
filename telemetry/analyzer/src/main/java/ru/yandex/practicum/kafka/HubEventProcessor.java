package ru.yandex.practicum.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.service.HubEventService;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    private final KafkaClient kafkaClient;
    private final HubEventService service;
    @Value("${topics.hub-event}")
    private String topic;

    @Override
    public void run() {
        log.info("Старт HubEventProcessor для топика '{}'", topic);
        Consumer<String, HubEventAvro> consumer = kafkaClient.getConsumerHubEvent();
        consumer.subscribe(List.of(topic));
        try {
            while (true) {
                log.debug("Ожидание сообщений Kafka...");
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro avroEvent = record.value();
                    String hubId = avroEvent.getHubId();

                    Object payload = avroEvent.getPayload();
                    log.info("Обрабатывается событие для hubId={}, payloadType={}", hubId, payload.getClass().getSimpleName());
                    switch (payload) {
                        case DeviceAddedEventAvro event -> service.deviceAdded(hubId, event);
                        case DeviceRemovedEventAvro event -> service.deviceRemoved(hubId, event);
                        case ScenarioAddedEventAvro event -> service.scenarioAdded(hubId, event);
                        case ScenarioRemovedEventAvro event -> service.scenarioRemoved(hubId, event);
                        default ->
                                throw new IllegalArgumentException("Неподдерживаемый тип payload: " + payload.getClass());
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка при обработке событий Kafka", e);
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
