package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;
import ru.yandex.practicum.exception.NoBookingFoundException;
import ru.yandex.practicum.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.ProductNotFoundException;
import ru.yandex.practicum.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.mapper.WarehouseMapper;
import ru.yandex.practicum.model.Dimension;
import ru.yandex.practicum.model.OrderBooking;
import ru.yandex.practicum.model.ProductInWarehouse;
import ru.yandex.practicum.repository.OrderBookingRepository;
import ru.yandex.practicum.repository.WarehouseRepository;

import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Service
@Slf4j
public class WarehouseServiceImpl implements WarehouseService {
    private final WarehouseRepository warehouseRepository;
    private final OrderBookingRepository orderBookingRepository;
    private final WarehouseMapper mapper;

    private static final String[] ADDRESSES = {"ADDRESS_1", "ADDRESS_2"};

    private static final String CURRENT_ADDRESS =
            ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

    @Override
    @Transactional
    public void createNewProductInWarehouse(NewProductInWarehouseRequest request) {
        log.info("Добавление новой продукции на склад");
        if (warehouseRepository.existsById(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException("Товар уже есть на складе");
        }
        warehouseRepository.save(mapper.toWarehouseProduct(request));
    }

    @Override
    @Transactional(readOnly = true)
    public BookedProductsDto checkProductQuantity(ShoppingCartDto cart) {
        log.info("Проверка доступности товаров из корзины: {}", cart.getShoppingCartId());
        Set<UUID> productIds = cart.getProducts().keySet();

        List<ProductInWarehouse> items =
                warehouseRepository.findAllByProductIdIn(productIds);

        Set<UUID> foundIds = items.stream()
                .map(ProductInWarehouse::getProductId)
                .collect(Collectors.toSet());

        Set<UUID> missingIds = new HashSet<>(productIds);
        missingIds.removeAll(foundIds);

        if (!missingIds.isEmpty()) {
            throw new ProductNotFoundException("Товары отсутствуют на складе: " + missingIds);
        }

        double totalWeight = 0;
        double totalVolume = 0;
        boolean fragile = false;

        Map<UUID, ProductInWarehouse> warehouseMap =
                items.stream()
                        .collect(Collectors.toMap(
                                ProductInWarehouse::getProductId,
                                Function.identity()
                        ));

        for (Map.Entry<UUID, Long> entry : cart.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            long quantityRequested = entry.getValue();

            ProductInWarehouse product = warehouseMap.get(productId);

            if (product.getQuantity() < quantityRequested) {
                throw new ProductNotFoundException(
                        "Недостаточно товара: " + productId
                );
            }

            double productVolume = calculatedVolume(product.getDimension(), productId);
            totalWeight += product.getWeight() * quantityRequested;
            totalVolume += productVolume * quantityRequested;

            if (product.isFragile()) {
                fragile = true;
            }
        }

        return new BookedProductsDto(
                totalWeight,
                totalVolume,
                fragile
        );
    }

    @Override
    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        log.info("Изменение количества продукции на складе");
        ProductInWarehouse product = warehouseRepository.findById(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException("На складе нет такого товара"));
        product.setQuantity(product.getQuantity() + request.getQuantity());
        warehouseRepository.save(product);
    }

    @Override
    public AddressDto getWarehouseAddress() {
        log.info("Получение адреса склада");
        return new AddressDto(
                CURRENT_ADDRESS,
                CURRENT_ADDRESS,
                CURRENT_ADDRESS,
                CURRENT_ADDRESS,
                CURRENT_ADDRESS
        );
    }

    @Override
    @Transactional
    //передача товаров в доставку
    public void shipToDelivery(ShippedToDeliveryRequest request) {
        log.info("Отправка товара в доставку {}, по заказу {}", request.getDeliveryId(), request.getOrderId());
        OrderBooking orderBooking = orderBookingRepository.findById(request.getOrderId())
                .orElseThrow(() -> new NoBookingFoundException("Не найден резерв с Id: " + request.getOrderId()));

        orderBooking.setDeliveryId(request.getDeliveryId());
        orderBookingRepository.save(orderBooking);
    }

    @Override
    @Transactional
    //Возвращаем товар на склад
    public void acceptReturn(Map<UUID, Long> returnedProducts) {
        log.info("Возврат продукции по списку: {}", returnedProducts);
        Map<UUID, ProductInWarehouse> products = warehouseRepository.findAllById(returnedProducts.keySet()).stream()
                .collect(Collectors.toMap(ProductInWarehouse::getProductId, Function.identity()));

        for (Map.Entry<UUID, Long> entry : returnedProducts.entrySet()) {
            if (!products.containsKey(entry.getKey())) {
                throw new ProductNotFoundException("Товар с id = " + entry.getKey() + " не найден на складе");
            }
            ProductInWarehouse product = products.get(entry.getKey());
            log.info("Товара {}, возвращено {}шт.", product.getProductId(), entry.getValue());
            product.setQuantity(product.getQuantity() + entry.getValue());
        }
        warehouseRepository.saveAll(products.values());
    }

    @Override
    @Transactional
    //Резервирование товара на складе
    public BookedProductsDto assembleProducts(AssemblyProductsForOrderRequest request) {
        log.info("Подготовка товаров к доставке по заказу {}", request.getOrderId());
        ShoppingCartDto shoppingCart = ShoppingCartDto.builder()
                .shoppingCartId(request.getOrderId())
                .products(request.getProducts())
                .build();

        BookedProductsDto bookedProductsDto = checkProductQuantity(shoppingCart);

        OrderBooking orderBooking = OrderBooking.builder()
                .orderId(request.getOrderId())
                .products(request.getProducts())
                .build();

        Map<UUID, Long> products = orderBooking.getProducts();
        List<ProductInWarehouse> productsInWarehouses = warehouseRepository.findAllByProductIdIn(products.keySet());

        if (productsInWarehouses.size() != products.size()) {
            throw new ProductNotFoundException("Не все товары найдены на складе");
        }

        for (ProductInWarehouse item : productsInWarehouses) {
            UUID productId = item.getProductId();
            Long orderedQuantity = products.get(productId);

            if (item.getQuantity() < orderedQuantity) {
                throw new ProductNotFoundException("Недостаточно товара на складе: " + productId);
            }

            item.setQuantity(item.getQuantity() - orderedQuantity);
        }

        warehouseRepository.saveAll(productsInWarehouses);

        orderBookingRepository.save(orderBooking);
        return bookedProductsDto;
    }

    private double calculatedVolume(Dimension dimension, UUID productId) {
        log.info("Расчет объема продукта (id): {}", productId);
        return dimension.getWidth() * dimension.getHeight() * dimension.getDepth();
    }
}
