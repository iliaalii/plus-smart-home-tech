package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.feign.OrderClient;
import ru.yandex.practicum.service.OrderService;

import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/order")
public class OrderController implements OrderClient {
    private final OrderService orderService;

    @Override
    public List<OrderDto> getClientOrders(String username) {
        checkUser(username);
        return orderService.getClientOrders(username);
    }

    @Override
    public OrderDto createNewOrder(@Valid CreateNewOrderRequest request) {
        return orderService.createNewOrder(request);
    }

    @Override
    public OrderDto productReturn(@Valid ProductReturnRequest request) {
        return orderService.productReturn(request);
    }

    @Override
    public OrderDto payment(UUID orderId) {
        return orderService.payment(orderId);
    }
    @Override
    public OrderDto paymentSuccess(UUID orderId) {
        return  orderService.paymentSuccess(orderId);
    }

    @Override
    public OrderDto paymentFailed(UUID orderId) {
        return orderService.paymentFailed(orderId);
    }

    @Override
    public OrderDto delivery(UUID orderId) {
        return orderService.delivery(orderId);
    }

    @Override
    public OrderDto deliveryFailed(UUID orderId) {
        return orderService.deliveryFailed(orderId);
    }

    @Override
    public OrderDto complete(UUID orderId) {
        return orderService.complete(orderId);
    }

    @Override
    public OrderDto calculateTotalCost(UUID orderId) {
        return orderService.calculateTotalCost(orderId);
    }

    @Override
    public OrderDto calculateDeliveryCost(UUID orderId) {
        return orderService.calculateDeliveryCost(orderId);
    }

    @Override
    public OrderDto assembly(UUID orderId) {
        return orderService.assembly(orderId);
    }

    @Override
    public OrderDto assemblyFailed(UUID orderId) {
        return orderService.assemblyFailed(orderId);
    }

    @Override
    public OrderDto sendingOnDelivery(UUID orderId) {
        return orderService.sendingOnDelivery(orderId);
    }

    private void checkUser(String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Пользователь не авторизован");
        }
    }
}
