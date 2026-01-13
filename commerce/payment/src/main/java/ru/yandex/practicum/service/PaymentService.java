package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

import java.util.UUID;

public interface PaymentService {
    PaymentDto createPayment(OrderDto orderDto);

    Double totalCost(OrderDto orderDto);

    void processSuccessfulPayment(UUID paymentId);

    Double productCost(OrderDto orderDto);

    void failedPayment(UUID paymentId);
}
