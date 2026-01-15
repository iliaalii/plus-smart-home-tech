package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.delivery.DeliveryState;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.ShippedToDeliveryRequest;
import ru.yandex.practicum.exception.NoDeliveryFoundException;
import ru.yandex.practicum.feign.OrderClient;
import ru.yandex.practicum.feign.WarehouseClient;
import ru.yandex.practicum.mapper.DeliveryMapper;
import ru.yandex.practicum.model.Delivery;
import ru.yandex.practicum.repository.DeliveryRepository;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeliveryServiceImpl implements DeliveryService {
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;
    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper mapper;

    private static final BigDecimal BASE_COST = BigDecimal.valueOf(5.0);
    private static final BigDecimal FRAGILE_RATE = BigDecimal.valueOf(0.2);
    private static final BigDecimal WEIGHT_RATE = BigDecimal.valueOf(0.3);
    private static final BigDecimal VOLUME_RATE = BigDecimal.valueOf(0.2);
    private static final BigDecimal STREET_RATE = BigDecimal.valueOf(0.2);

    @Override
    @Transactional
    //Создание новой заявки на доставку
    public DeliveryDto createDelivery(DeliveryDto deliveryDto) {
        log.info("Создание новой доставки {}", deliveryDto);
        Delivery delivery = mapper.toDelivery(deliveryDto);
        delivery.setDeliveryState(DeliveryState.CREATED);
        return mapper.toDto(deliveryRepository.save(delivery));
    }

    @Override
    @Transactional
    //подтверждение успешной доставки
    public void successfulDelivery(UUID orderId) {
        log.info("Изменение статуса доставки по заказу {}, на DELIVERED", orderId);
        Delivery delivery = findDeliveryByOrderId(orderId);

        log.info("Отправка запроса о замене статуса заказа {} на DELIVERY", orderId);
        orderClient.delivery(orderId);

        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);
    }

    @Override
    @Transactional
    //Получение товара в доставку
    public void pikedDelivery(UUID orderId) {
        log.info("Отправка товара в доставку по заказу {}", orderId);
        Delivery delivery = findDeliveryByOrderId(orderId);

        ShippedToDeliveryRequest request = ShippedToDeliveryRequest.builder()
                .deliveryId(delivery.getDeliveryId())
                .orderId(orderId)
                .build();

        warehouseClient.shipToDelivery(request);
        orderClient.sendingOnDelivery(orderId);

        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);
    }

    @Override
    @Transactional
    //Произошла ошибка при доставке
    public void failedDelivery(UUID orderId) {
        log.info("Изменение статуса доставки по заказу {}, на FAILED", orderId);
        Delivery delivery = findDeliveryByOrderId(orderId);

        log.info("Отправка запроса о замене статуса заказа {} на DELIVERY_FAILED", orderId);
        orderClient.deliveryFailed(orderId);

        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);
    }

    @Override
    @Transactional(readOnly = true)
    //Расчет стоимости доставки
    public BigDecimal calculateCost(OrderDto orderDto) {
        log.info("Расчет стоимости доставки по заказу {}", orderDto.getOrderId());
        Delivery delivery = findDeliveryByOrderId(orderDto.getOrderId());
        BigDecimal cost = BASE_COST;

        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
        String street = warehouseAddress.getStreet();
        log.info("Адрес склада для доставки выбран: {}", street);


        if ("ADDRESS_1".equals(street)) {
            cost = cost.multiply(BigDecimal.valueOf(1));
            log.info("К базовой стоимость доставки ({}) прибавляем налог склада  \"ADDRESS_1\", получив {}",
                    BASE_COST, cost);
        } else if ("ADDRESS_2".equals(street)) {
            cost = cost.add(cost.multiply(BigDecimal.valueOf(2)));
            log.info("К базовой стоимости доставки ({}) прибавляем налог склада \"ADDRESS_2\", получив {}",
                    BASE_COST, cost);
        }

        if (orderDto.getFragile()) {
            cost = cost.add(cost.multiply(FRAGILE_RATE));
            log.info("К стоимости доставки прибавляем налог (ставка {}) за хрупкость, получив {}",FRAGILE_RATE, cost);
        }

        cost = cost.add(BigDecimal.valueOf(orderDto.getDeliveryWeight())
                .multiply(WEIGHT_RATE));
        log.info("К стоимости доставки прибавляем налог (ставка {}) за вес, получив {}",WEIGHT_RATE, cost);

        cost = cost.add(BigDecimal.valueOf(orderDto.getDeliveryWeight())
                .multiply(VOLUME_RATE));
        log.info("К стоимости доставки прибавляем налог (ставка {}) за объем, получив {}",VOLUME_RATE, cost);


        if (!delivery.getToAddress().getStreet().equals(street)) {
            cost = cost.add(cost.multiply(STREET_RATE));
            log.info("Улица доставки и склада не совпадает, добавляем налог по курьеру (ставка {}), получив {}",
                    STREET_RATE, cost);
        }

        log.info("Общая стоимость доставки выход лит: {}", cost);
        return cost;
    }

    private Delivery findDeliveryByOrderId(UUID orderId) {
        return deliveryRepository.findById(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Не найдена доставка с ID: " + orderId));
    }
}
