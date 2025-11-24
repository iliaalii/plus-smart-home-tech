package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class SnapshotService {
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;
    private final HubRouterClient hubRouterClient;

    public void processSnapshot(SensorsSnapshotAvro snapshot) {
        log.info("Обработка снапшота хаба {} с {} датчиками",
                snapshot.getHubId(), snapshot.getSensorsState().size());

        Map<String, SensorStateAvro> sensorState = snapshot.getSensorsState();

        List<Scenario> scenarios = scenarioRepository.findByHubId(snapshot.getHubId());

        if (scenarios.isEmpty()) return;

        Map<Scenario, List<Condition>> conditionsMap =
                conditionRepository.findByScenarioIn(scenarios).stream()
                        .collect(Collectors.groupingBy(Condition::getScenario));

        Map<Scenario, List<Action>> actionsMap =
                actionRepository.findByScenarioIn(scenarios).stream()
                        .collect(Collectors.groupingBy(Action::getScenario));

        for (Scenario scenario : scenarios) {

            List<Condition> conds = conditionsMap.getOrDefault(scenario, List.of());

            if (checkScenario(conds, sensorState)) {
                actionsMap.getOrDefault(scenario, List.of())
                        .forEach(hubRouterClient::sendAction);
            }
        }
    }

    private boolean checkScenario(List<Condition> conditions,
                                  Map<String, SensorStateAvro> sensorState) {

        return conditions.stream().allMatch(condition ->
                checkCondition(condition, sensorState.get(condition.getSensor().getId())));
    }

    private boolean checkCondition(Condition condition, SensorStateAvro state) {
        if (state == null || state.getData() == null) return false;

        return switch (condition.getType()) {
            case MOTION -> checkValue(condition, ((MotionSensorAvro) state.getData()).getMotion() ? 1 : 0);
            case LUMINOSITY -> checkValue(condition, ((LightSensorAvro) state.getData()).getLuminosity());
            case SWITCH -> checkValue(condition, ((SwitchSensorAvro) state.getData()).getState() ? 1 : 0);
            case TEMPERATURE -> checkValue(condition, ((ClimateSensorAvro) state.getData()).getTemperatureC());
            case CO2LEVEL -> checkValue(condition, ((ClimateSensorAvro) state.getData()).getCo2Level());
            case HUMIDITY -> checkValue(condition, ((ClimateSensorAvro) state.getData()).getHumidity());
            default -> false;
        };
    }

    private boolean checkValue(Condition condition, Integer value) {
        Integer expected = condition.getIntValue();
        return switch (condition.getOperation()) {
            case EQUALS -> Objects.equals(value, expected);
            case GREATER_THAN -> value > expected;
            case LOWER_THAN -> value < expected;
            default -> false;
        };
    }
}
