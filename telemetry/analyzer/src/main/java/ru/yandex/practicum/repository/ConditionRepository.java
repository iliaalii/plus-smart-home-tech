package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;

import java.util.List;

public interface ConditionRepository extends JpaRepository<Condition, Long> {
    void deleteAllByScenario(Scenario scenario);

    List<Condition> findByScenarioIn(List<Scenario> scenarios);
}
