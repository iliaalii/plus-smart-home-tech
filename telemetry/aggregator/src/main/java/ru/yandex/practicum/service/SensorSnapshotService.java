package ru.yandex.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class SensorSnapshotService {
    private final Map<String, SensorsSnapshotAvro> sensorsSnapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        log.info("Получено состояние события: {}", event);
        SensorsSnapshotAvro snapshot = sensorsSnapshots.computeIfAbsent(event.getHubId(), hubId ->
                SensorsSnapshotAvro.newBuilder()
                        .setHubId(hubId)
                        .setSensorsState(new HashMap<>())
                        .setTimestamp(event.getTimestamp())
                        .build()
        );
        log.info("Создан новый снапшот: {}", snapshot);

        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());
        log.info("Проверка прошлого состояния");
        if (oldState != null &&
                (oldState.getTimestamp().isAfter(event.getTimestamp()) ||
                        oldState.getData().equals(event.getPayload()))) {
            log.info("Обновление не требуется");
            return Optional.empty();
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());
        log.info("Снапшот обновлён: {}", snapshot);
        return Optional.of(snapshot);
    }
}
