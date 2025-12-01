package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Sensor;


public interface SensorRepository extends JpaRepository<Sensor, String> {
}
