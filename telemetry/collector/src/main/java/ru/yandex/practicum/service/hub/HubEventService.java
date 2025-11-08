package ru.yandex.practicum.service.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.hub.*;
import ru.yandex.practicum.service.EventService;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService implements EventService<HubEvent> {
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Value("${topics.hub-event}")
    private String topic;

    @Override
    public void send(HubEvent event) {
        HubEventAvro hubEventAvro = toAvro(event);
        kafkaTemplate.send(topic, hubEventAvro);
        log.info("Ивент: {}, отправлен в топик: {}", event, topic);
    }

    private HubEventAvro toAvro(HubEvent event) {
        log.info("Определение формата ивента");
        Object payload = switch (event.getType()) {
            case DEVICE_ADDED -> toDeviceAddedAvro((DeviceAddedEvent) event);
            case DEVICE_REMOVED -> toDeviceRemovedAvro((DeviceRemovedEvent) event);
            case SCENARIO_ADDED -> toScenarioAddedAvro((ScenarioAddedEvent) event);
            case SCENARIO_REMOVED -> toScenarioRemovedAvro((ScenarioRemovedEvent) event);
            default -> throw new IllegalArgumentException("Неподдерживаемый тип ивента: " + event.getType());
        };

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private DeviceAddedEventAvro toDeviceAddedAvro(DeviceAddedEvent event) {
        log.info("Определен формат Device Added,переводим в Avro..");
        return DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(DeviceTypeAvro.valueOf(event.getDeviceType().name()))
                .build();
    }

    private DeviceRemovedEventAvro toDeviceRemovedAvro(DeviceRemovedEvent event) {
        log.info("Определен формат Device Removed,переводим в Avro..");
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    private ScenarioAddedEventAvro toScenarioAddedAvro(ScenarioAddedEvent event) {
        log.info("Определен формат Scenario Added,переводим в Avro..");
        List<DeviceActionAvro> deviceActionAvroList = event.getActions().stream()
                .map(this::toDeviceActionAvro)
                .toList();
        List<ScenarioConditionAvro> scenarioConditionAvroList = event.getConditions().stream()
                .map(this::toScenarioConditionAvro)
                .toList();
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setActions(deviceActionAvroList)
                .setConditions(scenarioConditionAvroList)
                .build();
    }

    private ScenarioRemovedEventAvro toScenarioRemovedAvro(ScenarioRemovedEvent event) {
        log.info("Определен формат Scenario Removed,переводим в Avro..");
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();
    }

    private DeviceActionAvro toDeviceActionAvro(DeviceAction action) {
        log.info("Перевод типа действия устройства в Avro");
        return DeviceActionAvro.newBuilder()
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setSensorId(action.getSensorId())
                .setValue(action.getValue())
                .build();
    }

    private ScenarioConditionAvro toScenarioConditionAvro(ScenarioCondition condition) {
        log.info("Перевод условий сценария в Avro");
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setValue(condition.getValue())
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .build();
    }
}
