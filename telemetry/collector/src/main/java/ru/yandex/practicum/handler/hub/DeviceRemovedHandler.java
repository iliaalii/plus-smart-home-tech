package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.KafkaClient;
import ru.yandex.practicum.mapper.HubEventMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRemovedHandler implements HubEventHandler {
    private final KafkaClient kafkaClient;
    private final HubEventMapper hubEventMapper;

    @Value("${topics.hub-event}")
    private String topic;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    public void handle(HubEventProto eventProto) {
        kafkaClient.getProducer().send(new ProducerRecord<>(topic, hubEventMapper.toAvro(eventProto)));
        log.info("Ивент: {}, отправлен в топик: {}", eventProto, topic);
    }
}