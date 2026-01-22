package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.OrderState;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.warehouse.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.exception.NoOrderFoundException;
import ru.yandex.practicum.feign.DeliveryClient;
import ru.yandex.practicum.feign.PaymentClient;
import ru.yandex.practicum.feign.WarehouseClient;
import ru.yandex.practicum.mapper.OrderMapper;
import ru.yandex.practicum.model.Order;
import ru.yandex.practicum.repository.OrderRepository;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl implements OrderService {
    private final DeliveryClient deliveryClient;
    private final PaymentClient paymentClient;
    private final WarehouseClient warehouseClient;
    private final OrderRepository orderRepository;
    private final OrderMapper mapper;

    @Override
    @Transactional(readOnly = true)
    //получение всех заказов пользователя
    public List<OrderDto> getClientOrders(String username) {
        log.info("Поиск всех заказов пользователя: {}", username);
        return orderRepository.findAllByUsername(username).stream()
                .map(mapper::toDto)
                .toList();
    }

    @Override
    @Transactional
    //Создаем новый заказ
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        log.info("Создание нового заказа по корзине: {}", request.getShoppingCart().getShoppingCartId());
        Order order = Order.builder()
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .products(request.getShoppingCart().getProducts())
                .username(request.getUsername())
                .state(OrderState.NEW)
                .build();
        order = orderRepository.save(order);

        DeliveryDto deliveryDto = DeliveryDto.builder()
                .orderId(order.getOrderId())
                .toAddress(request.getDeliveryAddress())
                .build();

        log.info("Отправляем запрос на создание доставки {}", deliveryDto);
        deliveryDto = deliveryClient.createDelivery(deliveryDto);

        log.info("Получили номер доставки {}", deliveryDto.getDeliveryId());
        order.setDeliveryId(deliveryDto.getDeliveryId());

        log.info("Отправляем запрос на расчет стоимости всей продукции по заказу {}", order.getOrderId());
        order.setProductPrice(paymentClient.productCost(mapper.toDto(order)));

        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Возврат заказа
    public OrderDto productReturn(ProductReturnRequest request) {
        log.info("Возврат заказа: {}", request.getOrderId());
        Order order = findOrder(request.getOrderId());
        warehouseClient.acceptReturn(request.getProducts());

        log.info("Статус заказа {}, изменен на PRODUCT_RETURNED", request.getOrderId());
        order.setState(OrderState.PRODUCT_RETURNED);

        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Создание оплаты товара
    public OrderDto payment(UUID orderId) {
        log.info("Отправляем заказ {}, на оплату", orderId);
        Order order = findOrder(orderId);
        PaymentDto paymentDto = paymentClient.createPayment(mapper.toDto(order));

        log.info("Получен номер платежа: {}", paymentDto.getPaymentId());
        order.setPaymentId(paymentDto.getPaymentId());

        log.info("Статус заказа {}, изменен на ON_PAYMENT", orderId);
        order.setState(OrderState.ON_PAYMENT);

        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Изменение статуса при оплате платежа
    public OrderDto paymentSuccess(UUID orderId) {
        log.info("Запрос на изменение статуса заказа {}, на PAID", orderId);
        Order order = findOrder(orderId);
        order.setState(OrderState.PAID);
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Изменение статуса при ошибке платежа
    public OrderDto paymentFailed(UUID orderId) {
        log.info("Запрос на изменение статуса заказа {}, на PAYMENT_FAILED", orderId);
        Order order = findOrder(orderId);
        order.setState(OrderState.PAYMENT_FAILED);
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Изменение статуса о выполненной доставке
    public OrderDto delivery(UUID orderId) {
        log.info("Статус заказа {}, изменен на DELIVERED", orderId);
        Order order = findOrder(orderId);
        order.setState(OrderState.DELIVERED);
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Изменение статуса при ошибке доставки
    public OrderDto deliveryFailed(UUID orderId) {
        log.info("Запрос на изменение статуса заказа {}, на DELIVERY_FAILED", orderId);
        Order order = findOrder(orderId);
        order.setState(OrderState.DELIVERY_FAILED);
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Изменение статуса при выполненном заказе
    public OrderDto complete(UUID orderId) {
        log.info("Запрос на изменение статуса заказа {}, на COMPLETED", orderId);
        Order order = findOrder(orderId);
        order.setState(OrderState.COMPLETED);
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Расчет стоимости всего заказа
    public OrderDto calculateTotalCost(UUID orderId) {
        log.info("Расчет полной стоимости заказа {}", orderId);
        Order order = findOrder(orderId);
        order.setTotalPrice(paymentClient.totalCost(mapper.toDto(order)));
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Расчет стоимости доставки
    public OrderDto calculateDeliveryCost(UUID orderId) {
        log.info("Расчет стоимости доставки заказа {}", orderId);
        Order order = findOrder(orderId);
        order.setDeliveryPrice(deliveryClient.calculateCost(mapper.toDto(order)));
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Сборка заказа в доставку
    public OrderDto assembly(UUID orderId) {
        log.info("Отправка заказа {}, на сборку товаров", orderId);
        Order order = findOrder(orderId);

        AssemblyProductsForOrderRequest request = AssemblyProductsForOrderRequest.builder()
                .orderId(orderId)
                .products(order.getProducts())
                .build();

        log.info("Отправляем запрос на создание сборки {}", request);
        BookedProductsDto bookedProductsDto = warehouseClient.assembleProducts(request);

        order.setDeliveryVolume(bookedProductsDto.getDeliveryVolume());
        order.setDeliveryWeight(bookedProductsDto.getDeliveryWeight());
        order.setFragile(bookedProductsDto.isFragile());

        log.info("Статус заказа {}, изменен на ASSEMBLED", orderId);
        order.setState(OrderState.ASSEMBLED);
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Изменение статуса при ошибке сборки
    public OrderDto assemblyFailed(UUID orderId) {
        log.info("Запрос на изменение статуса заказа {}, на ASSEMBLY_FAILED", orderId);
        Order order = findOrder(orderId);
        order.setState(OrderState.ASSEMBLY_FAILED);
        return mapper.toDto(orderRepository.save(order));
    }

    @Override
    @Transactional
    //Изменение на статус в доставке
    public OrderDto sendingOnDelivery(UUID orderId) {
        log.info("Статус заказа {}, изменен на ON_DELIVERY", orderId);
        Order order = findOrder(orderId);
        order.setState(OrderState.ON_DELIVERY);
        return mapper.toDto(orderRepository.save(order));
    }

    //Поиск заказа по id
    private Order findOrder(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Нету заказа с ID: " + orderId));
    }
}
