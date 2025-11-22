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

import java.util.List;

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

        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            sensorRepository.findById(conditionAvro.getSensorId()).ifPresent(sensor -> {
                Condition condition = new Condition();
                condition.setScenario(scenario);
                condition.setSensor(sensor);
                condition.setType(conditionAvro.getType());
                condition.setOperation(conditionAvro.getOperation());
                if (conditionAvro.getValue() instanceof Integer intVal) {
                    condition.setIntValue(intVal);
                } else if (conditionAvro.getValue() instanceof Boolean boolVal) {
                    condition.setIntValue(boolVal ? 1 : 0);
                }
                conditionRepository.save(condition);
            });
        }

        for (DeviceActionAvro actionAvro : event.getActions()) {
            sensorRepository.findById(actionAvro.getSensorId()).ifPresent(sensor -> {
                Action action = new Action();
                action.setScenario(scenario);
                action.setSensor(sensor);
                action.setType(actionAvro.getType());
                if (actionAvro.getValue() != null) {
                    action.setValue(actionAvro.getValue());
                }
                actionRepository.save(action);
            });
        }
    }

    public void scenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        log.info("Удаление сценария '{}' из хаба {}", event.getName(), hubId);
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        for (Scenario scenario : scenarios) {
            if (scenario.getName().equals(event.getName())) {
                conditionRepository.deleteAllByScenario(scenario);
                actionRepository.deleteAllByScenario(scenario);
                scenarioRepository.delete(scenario);
            }
        }
    }
}
