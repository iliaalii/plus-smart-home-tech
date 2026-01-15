package ru.yandex.practicum.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

import java.math.BigDecimal;
import java.util.UUID;

@FeignClient(name = "payment", path = "/api/v1/payment")
public interface PaymentClient {
    @PostMapping
    PaymentDto createPayment(@RequestBody OrderDto orderDto);

    @PostMapping("/totalCost")
    BigDecimal totalCost(@RequestBody OrderDto orderDto);

    @PostMapping("/refund")
    void processSuccessfulPayment(@RequestBody UUID paymentId);

    @PostMapping("/productCost")
    BigDecimal productCost(@RequestBody OrderDto orderDto);

    @PostMapping("/failed")
    void failedPayment(@RequestBody UUID paymentId);
}
