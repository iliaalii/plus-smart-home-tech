package ru.yandex.practicum.mapper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Slf4j
@Component
public class SensorEventMapper {
    public SensorEventAvro toAvro(SensorEventProto event) {
        log.info("Определение формата ивента");
        Object payload = switch (event.getPayloadCase()) {
            case CLIMATE_SENSOR -> toClimateSensorAvro(event.getClimateSensor());
            case LIGHT_SENSOR -> toLightSensorAvro(event.getLightSensor());
            case MOTION_SENSOR -> toMotionSensorAvro(event.getMotionSensor());
            case SWITCH_SENSOR -> toSwitchSensorAvro(event.getSwitchSensor());
            case TEMPERATURE_SENSOR -> toTemperatureSensorAvro(event.getTemperatureSensor());
            default -> throw new IllegalArgumentException("Неподдерживаемый тип ивента: " + event.getPayloadCase());
        };
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    private ClimateSensorAvro toClimateSensorAvro(ClimateSensorProto event) {
        log.info("Определен датчик Climate Sensor,переводим в Avro..");
        return ClimateSensorAvro.newBuilder()
                .setCo2Level(event.getCo2Level())
                .setHumidity(event.getHumidity())
                .setTemperatureC(event.getTemperatureC())
                .build();
    }

    private LightSensorAvro toLightSensorAvro(LightSensorProto event) {
        log.info("Определен датчик Light Sensor,переводим в Avro..");
        return LightSensorAvro.newBuilder()
                .setLuminosity(event.getLuminosity())
                .setLinkQuality(event.getLinkQuality())
                .build();
    }

    private MotionSensorAvro toMotionSensorAvro(MotionSensorProto event) {
        log.info("Определен датчик Motion Sensor,переводим в Avro..");
        return MotionSensorAvro.newBuilder()
                .setMotion(event.getMotion())
                .setLinkQuality(event.getLinkQuality())
                .setVoltage(event.getVoltage())
                .build();
    }

    private SwitchSensorAvro toSwitchSensorAvro(SwitchSensorProto event) {
        log.info("Определен датчик Switch Sensor,переводим в Avro..");
        return SwitchSensorAvro.newBuilder()
                .setState(event.getState())
                .build();
    }

    private TemperatureSensorAvro toTemperatureSensorAvro(TemperatureSensorProto event) {
        log.info("Определен датчик Temperature Sensor,переводим в Avro..");
        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setTemperatureF(event.getTemperatureF())
                .build();
    }
}
