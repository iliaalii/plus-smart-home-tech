package ru.yandex.practicum.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryClient {
    @PutMapping
    DeliveryDto createDelivery(@RequestBody DeliveryDto deliveryDto);

    @PostMapping("/successful")
    void successfulDelivery(@RequestBody UUID orderId);

    @PostMapping("/picked")
    void pikedDelivery(@RequestBody UUID orderId);

    @PostMapping("/failed")
    void failedDelivery(@RequestBody UUID orderId);

    @PostMapping("/cost")
    BigDecimal calculateCost(@RequestBody OrderDto orderDto);
}
