package ru.yandex.practicum.service.handler.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.KafkaClient;
import ru.yandex.practicum.mapper.SensorEventMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class MotionSensorHandler implements SensorEventHandler {
    private final KafkaClient kafkaClient;
    private final SensorEventMapper sensorEventMapper;

    @Value("${topics.sensor-event}")
    private String topic;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    public void handle(SensorEventProto eventProto) {
        kafkaClient.getProducer().send(new ProducerRecord<>(topic, sensorEventMapper.toAvro(eventProto)));
        log.info("Ивент: {}, отправлен в топик: {}", eventProto, topic);
    }
}
