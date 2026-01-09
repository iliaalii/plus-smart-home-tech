package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.ProductInWarehouse;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface WarehouseRepository extends JpaRepository<ProductInWarehouse, UUID> {
    List<ProductInWarehouse> findAllByProductIdIn(Set<UUID> productIds);
}
