package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;

import java.util.Map;
import java.util.UUID;

public interface WarehouseService {
    void createNewProductInWarehouse(NewProductInWarehouseRequest request);

    BookedProductsDto checkProductQuantity(ShoppingCartDto cart);

    void addProductToWarehouse(AddProductToWarehouseRequest request);

    AddressDto getWarehouseAddress();

    void shipToDelivery(ShippedToDeliveryRequest request);

    void acceptReturn(Map<UUID, Long> returnedProducts);

    BookedProductsDto assembleProducts(AssemblyProductsForOrderRequest request);
}
