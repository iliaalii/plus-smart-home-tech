package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class HubEventService {
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    public void deviceAdded(String hubId, DeviceAddedEventAvro event) {
        log.info("Добавление устройства {} в хаб {}", event.getId(), hubId);
        if (!sensorRepository.existsById(event.getId())) {
            Sensor sensor = new Sensor();
            sensor.setId(event.getId());
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            log.debug("Устройство {} добавлено в базу", event.getId());
        } else {
            log.debug("Устройство {} уже существует в базе, пропуск добавления", event.getId());
        }
    }

    public void deviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        log.info("Удаление устройства {} из хаба {}", event.getId(), hubId);
        sensorRepository.findById(event.getId()).ifPresent(sensorRepository::delete);
    }

    public void scenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        log.info("Добавление сценария '{}' в хаб {}", event.getName(), hubId);
        if (scenarioRepository.existsByHubIdAndName(hubId, event.getName())) {
            return;
        }

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName());
        scenarioRepository.save(scenario);
        log.debug("Сценарий '{}' создан в базе", event.getName());

        List<String> sensorIds = new ArrayList<>();
        event.getConditions().forEach(condition -> sensorIds.add(condition.getSensorId()));
        event.getActions().forEach(action -> sensorIds.add(action.getSensorId()));

        Map<String, Sensor> sensors = sensorRepository
                .findAllById(sensorIds)
                .stream()
                .collect(Collectors.toMap(Sensor::getId, s -> s));

        for (ScenarioConditionAvro c : event.getConditions()) {
            Sensor sensor = sensors.get(c.getSensorId());
            if (sensor == null) {
                log.warn("Сенсор {} отсутствует, условие пропущено", c.getSensorId());
                continue;
            }

            Condition condition = new Condition();
            condition.setScenario(scenario);
            condition.setSensor(sensor);
            condition.setType(c.getType());
            condition.setOperation(c.getOperation());

            if (c.getValue() instanceof Integer i) {
                condition.setIntValue(i);
            } else if (c.getValue() instanceof Boolean b) {
                condition.setIntValue(b ? 1 : 0);
            }

            conditionRepository.save(condition);
        }

        for (DeviceActionAvro a : event.getActions()) {
            Sensor sensor = sensors.get(a.getSensorId());
            if (sensor == null) {
                log.warn("Сенсор {} отсутствует, действие пропущено", a.getSensorId());
                continue;
            }

            Action action = new Action();
            action.setScenario(scenario);
            action.setSensor(sensor);
            action.setType(a.getType());
            action.setValue(a.getValue());

            actionRepository.save(action);
        }

        log.info("Сценарий '{}' полностью сохранён", event.getName());
    }

    public void scenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        log.info("Удаление сценария '{}' из хаба {}", event.getName(), hubId);

        Optional<Scenario> optionalScenario =
                scenarioRepository.findByHubIdAndName(hubId, event.getName());

        if (optionalScenario.isEmpty()) {
            log.info("Сценарий '{}' не найден", event.getName());
            return;
        }

        Scenario scenario = optionalScenario.get();

        conditionRepository.deleteAllByScenario(scenario);
        actionRepository.deleteAllByScenario(scenario);
        scenarioRepository.delete(scenario);

        log.info("Сценарий '{}' удалён", event.getName());
    }
}
