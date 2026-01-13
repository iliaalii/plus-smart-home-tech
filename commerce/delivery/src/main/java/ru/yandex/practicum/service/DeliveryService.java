package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;

import java.util.UUID;

public interface DeliveryService {
    DeliveryDto createDelivery(DeliveryDto deliveryDto);

    void successfulDelivery(UUID orderId);

    void pikedDelivery(UUID orderId);

    void failedDelivery(UUID orderId);

    Double calculateCost(OrderDto orderDto);
}
