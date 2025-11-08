package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.sensor.*;
import ru.yandex.practicum.service.EventService;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventService implements EventService<SensorEvent> {
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Value("${topics.sensor-event}")
    private String topic;

    @Override
    public void send(SensorEvent event) {
        SensorEventAvro sensorEventAvro = toAvro(event);
        kafkaTemplate.send(topic, sensorEventAvro);
        log.info("Ивент: {}, отправлен в топик: {}", event, topic);
    }

    private SensorEventAvro toAvro(SensorEvent event) {
        log.info("Определение формата ивента");
        Object payload = switch (event.getType()) {
            case CLIMATE_SENSOR_EVENT -> toClimateSensorAvro((ClimateSensorEvent) event);
            case LIGHT_SENSOR_EVENT -> toLightSensorAvro((LightSensorEvent) event);
            case MOTION_SENSOR_EVENT -> toMotionSensorAvro((MotionSensorEvent) event);
            case SWITCH_SENSOR_EVENT -> toSwitchSensorAvro((SwitchSensorEvent) event);
            case TEMPERATURE_SENSOR_EVENT -> toTemperatureSensorAvro((TemperatureSensorEvent) event);
            default -> throw new IllegalArgumentException("Неподдерживаемый тип ивента: " + event.getClass().getName());
        };

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private ClimateSensorAvro toClimateSensorAvro(ClimateSensorEvent event) {
        log.info("Определен датчик Climate Sensor,переводим в Avro..");
        return ClimateSensorAvro.newBuilder()
                .setCo2Level(event.getCo2Level())
                .setHumidity(event.getHumidity())
                .setTemperatureC(event.getTemperatureC())
                .build();
    }

    private LightSensorAvro toLightSensorAvro(LightSensorEvent event) {
        log.info("Определен датчик Light Sensor,переводим в Avro..");
        return LightSensorAvro.newBuilder()
                .setLuminosity(event.getLuminosity())
                .setLinkQuality(event.getLinkQuality())
                .build();
    }

    private MotionSensorAvro toMotionSensorAvro(MotionSensorEvent event) {
        log.info("Определен датчик Motion Sensor,переводим в Avro..");
        return MotionSensorAvro.newBuilder()
                .setMotion(event.getMotion())
                .setLinkQuality(event.getLinkQuality())
                .setVoltage(event.getVoltage())
                .build();
    }

    private SwitchSensorAvro toSwitchSensorAvro(SwitchSensorEvent event) {
        log.info("Определен датчик Switch Sensor,переводим в Avro..");
        return SwitchSensorAvro.newBuilder()
                .setState(event.getState())
                .build();
    }

    private TemperatureSensorAvro toTemperatureSensorAvro(TemperatureSensorEvent event) {
        log.info("Определен датчик Temperature Sensor,переводим в Avro..");
        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setTemperatureF(event.getTemperatureF())
                .build();
    }
}
