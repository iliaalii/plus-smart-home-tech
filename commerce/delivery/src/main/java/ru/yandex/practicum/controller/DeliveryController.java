package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.feign.DeliveryClient;
import ru.yandex.practicum.service.DeliveryService;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/delivery")
public class DeliveryController implements DeliveryClient {
    private final DeliveryService deliveryService;

    @Override
    public DeliveryDto createDelivery(@Valid DeliveryDto deliveryDto) {
        return deliveryService.createDelivery(deliveryDto);
    }

    @Override
    public void successfulDelivery(UUID orderId) {
        deliveryService.successfulDelivery(orderId);
    }

    @Override
    public void pikedDelivery(UUID orderId) {
        deliveryService.pikedDelivery(orderId);
    }

    @Override
    public void failedDelivery(UUID orderId) {
        deliveryService.failedDelivery(orderId);
    }

    @Override
    public BigDecimal calculateCost(@Valid OrderDto orderDto) {
        validateOrderForCostDelivery(orderDto);
        return deliveryService.calculateCost(orderDto);
    }

    private void validateOrderForCostDelivery(OrderDto orderDto) {
        if (orderDto.getFragile() == null) {
            throw new IllegalArgumentException("Хрупкость должна быть указана");
        }
        if (orderDto.getDeliveryVolume() == null) {
            throw new IllegalArgumentException("Объем должен быть указан");
        }
        if (orderDto.getDeliveryWeight() == null) {
            throw new IllegalArgumentException("Вес должен быть указан");
        }
    }
}
