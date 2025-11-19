package ru.yandex.practicum.mapper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component
public class HubEventMapper {
    public HubEventAvro toAvro(HubEventProto event) {
        log.info("Определение формата ивента");
        Object payload = switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> toDeviceAddedAvro(event.getDeviceAdded());
            case DEVICE_REMOVED -> toDeviceRemovedAvro(event.getDeviceRemoved());
            case SCENARIO_ADDED -> toScenarioAddedAvro(event.getScenarioAdded());
            case SCENARIO_REMOVED -> toScenarioRemovedAvro(event.getScenarioRemoved());
            default -> throw new IllegalArgumentException("Неподдерживаемый тип ивента: " + event.getPayloadCase());
        };
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    private DeviceAddedEventAvro toDeviceAddedAvro(DeviceAddedEventProto event) {
        log.info("Определен формат Device Added,переводим в Avro..");
        return DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(DeviceTypeAvro.valueOf(event.getType().name()))
                .build();
    }

    private DeviceRemovedEventAvro toDeviceRemovedAvro(DeviceRemovedEventProto event) {
        log.info("Определен формат Device Removed,переводим в Avro..");
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    private ScenarioAddedEventAvro toScenarioAddedAvro(ScenarioAddedEventProto event) {
        log.info("Определен формат Scenario Added,переводим в Avro..");
        List<DeviceActionAvro> deviceActionAvroList = event.getActionList().stream()
                .map(this::toDeviceActionAvro)
                .toList();
        List<ScenarioConditionAvro> scenarioConditionAvroList = event.getConditionList().stream()
                .map(this::toScenarioConditionAvro)
                .toList();
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setActions(deviceActionAvroList)
                .setConditions(scenarioConditionAvroList)
                .build();
    }

    private ScenarioRemovedEventAvro toScenarioRemovedAvro(ScenarioRemovedEventProto event) {
        log.info("Определен формат Scenario Removed,переводим в Avro..");
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();
    }

    private DeviceActionAvro toDeviceActionAvro(DeviceActionProto action) {
        log.info("Перевод тип действия устройства в Avro");
        return DeviceActionAvro.newBuilder()
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setSensorId(action.getSensorId())
                .setValue(action.hasValue() ? action.getValue() : null)
                .build();
    }

    private ScenarioConditionAvro toScenarioConditionAvro(ScenarioConditionProto condition) {
        log.info("Перевод условия сценария в Avro");
        Object value = switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();
            case INT_VALUE -> condition.getIntValue();
            case VALUE_NOT_SET -> null;
        };
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setValue(value)
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .build();
    }
}
