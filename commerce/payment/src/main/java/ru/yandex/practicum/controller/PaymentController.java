package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.feign.PaymentClient;
import ru.yandex.practicum.service.PaymentService;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/payment")
public class PaymentController implements PaymentClient {
    private final PaymentService paymentService;

    @Override
    public PaymentDto createPayment(@Valid OrderDto orderDto) {
        validateOrder(orderDto);
        return paymentService.createPayment(orderDto);
    }

    @Override
    public BigDecimal totalCost(@Valid OrderDto orderDto) {
        validateOrderForCost(orderDto);
        return paymentService.totalCost(orderDto);
    }

    @Override
    public void processSuccessfulPayment(UUID paymentId) {
        paymentService.processSuccessfulPayment(paymentId);
    }

    @Override
    public BigDecimal productCost(@Valid OrderDto orderDto) {
        return paymentService.productCost(orderDto);
    }

    @Override
    public void failedPayment(UUID paymentId) {
        paymentService.failedPayment(paymentId);
    }

    private void validateOrder(OrderDto orderDto) {
        if (orderDto.getTotalPrice() == null)
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость заказа");
        if (orderDto.getDeliveryPrice() == null)
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость доставки");
        if (orderDto.getProductPrice() == null)
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость товаров");
    }

    private void validateOrderForCost(OrderDto orderDto) {
        if (orderDto.getDeliveryPrice() == null)
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость доставки");
        if (orderDto.getProductPrice() == null)
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость товаров");
    }
}
