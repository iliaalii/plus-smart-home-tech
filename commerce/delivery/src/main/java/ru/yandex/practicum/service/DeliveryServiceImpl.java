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

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeliveryServiceImpl implements DeliveryService {
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;
    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper mapper;

    private static final Double BASE_COST = 5.0;
    private static final Double FRAGILE_RATE = 0.2;
    private static final Double WEIGHT_RATE = 0.3;
    private static final Double VOLUME_RATE = 0.2;
    private static final Double STREET_RATE = 0.2;

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
    public Double calculateCost(OrderDto orderDto) {
        log.info("Расчет стоимости доставки по заказу {}", orderDto.getOrderId());
        Delivery delivery = findDeliveryByOrderId(orderDto.getOrderId());
        double cost = BASE_COST;

        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
        String street = warehouseAddress.getStreet();
        log.info("Адрес склада для доставки выбран: {}", street);

        if ("ADDRESS_1".equals(street)) {
            cost *= 1;
        } else if ("ADDRESS_2".equals(street)) {
            cost += cost * 2;
        }

        if (orderDto.getFragile()) {
            cost += cost * FRAGILE_RATE;
        }

        cost += orderDto.getDeliveryWeight() * WEIGHT_RATE;
        cost += orderDto.getDeliveryVolume() * VOLUME_RATE;

        if (!delivery.getToAddress().getStreet().equals(street)) {
            cost += cost * STREET_RATE;
        }
        return cost;
    }

    private Delivery findDeliveryByOrderId(UUID orderId) {
        return deliveryRepository.findById(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Не найдена доставка с ID: " + orderId));
    }
}
