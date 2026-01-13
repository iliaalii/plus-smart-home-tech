package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.payment.PaymentState;
import ru.yandex.practicum.exception.NoOrderFoundException;
import ru.yandex.practicum.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.feign.OrderClient;
import ru.yandex.practicum.feign.ShoppingStoreClient;
import ru.yandex.practicum.mapper.PaymentMapper;
import ru.yandex.practicum.model.Payment;
import ru.yandex.practicum.repository.PaymentRepository;

import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentServiceImpl implements PaymentService {
    private final OrderClient orderClient;
    private final ShoppingStoreClient storeClient;
    private final PaymentRepository paymentRepository;
    private final PaymentMapper mapper;

    private static final Double fee = 0.1;

    @Override
    @Transactional
    //Создаем оплату
    public PaymentDto createPayment(OrderDto orderDto) {
        log.info("Создание нового платежа для заказа {}", orderDto.getOrderId());
        Payment payment = Payment.builder()
                .orderId(orderDto.getOrderId())
                .productPrice(orderDto.getProductPrice())
                .deliveryTotal(orderDto.getDeliveryPrice())
                .totalPayment(orderDto.getTotalPrice())
                .feeTotal(orderDto.getTotalPrice() * fee)
                .paymentStatus(PaymentState.PENDING)
                .build();
        return mapper.toDto(paymentRepository.save(payment));
    }

    @Override
    @Transactional(readOnly = true)
    //Расчет стоимости заказа
    public Double totalCost(OrderDto orderDto) {
        log.info("Получение полной стоимости платежа по заказу {}", orderDto.getOrderId());
        return orderDto.getProductPrice() + orderDto.getDeliveryPrice() + orderDto.getProductPrice() * fee;
    }

    @Override
    @Transactional
    //Платеж прошел успешно
    public void processSuccessfulPayment(UUID paymentId) {
        Payment payment = findPayment(paymentId);
        orderClient.paymentSuccess(payment.getOrderId());
        log.info("Статус платежа {} изменен на SUCCESS", paymentId);
        payment.setPaymentStatus(PaymentState.SUCCESS);
        paymentRepository.save(payment);
    }

    @Override
    @Transactional(readOnly = true)
    //Получение стоимости всех товаров
    public Double productCost(OrderDto orderDto) {
        log.info("Расчет стоимости продукции по заказу {}", orderDto.getOrderId());
        Map<UUID, Long> products = orderDto.getProducts();
        Map<UUID, Double> prices = storeClient.getPrices(products.keySet());
        double total = 0.0;

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            Double price = prices.get(entry.getKey());
            if (price == null) {
                throw new NotEnoughInfoInOrderToCalculateException("Не найден ценник для продукта: " + entry.getKey());
            }
            total += price * entry.getValue();
        }

        return total;
    }

    @Override
    @Transactional
    //При платеже произошла ошибка
    public void failedPayment(UUID paymentId) {
        log.info("Статус платежа {} изменен на FAILED", paymentId);
        Payment payment = findPayment(paymentId);
        orderClient.paymentFailed(payment.getOrderId());
        payment.setPaymentStatus(PaymentState.FAILED);
        paymentRepository.save(payment);
    }

    private Payment findPayment(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoOrderFoundException("Не найден платеж: " + paymentId));
    }
}
