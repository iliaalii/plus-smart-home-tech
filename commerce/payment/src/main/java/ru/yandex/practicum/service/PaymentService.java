package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

import java.math.BigDecimal;
import java.util.UUID;

public interface PaymentService {
    PaymentDto createPayment(OrderDto orderDto);

    BigDecimal totalCost(OrderDto orderDto);

    void processSuccessfulPayment(UUID paymentId);

    BigDecimal productCost(OrderDto orderDto);

    void failedPayment(UUID paymentId);
}
