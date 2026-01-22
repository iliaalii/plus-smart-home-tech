package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;
import ru.yandex.practicum.feign.WarehouseClient;
import ru.yandex.practicum.service.WarehouseService;

import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/warehouse")
public class WarehouseController implements WarehouseClient {
    private final WarehouseService warehouseService;

    @Override
    public void createNewProductInWarehouse(@Valid NewProductInWarehouseRequest request) {
        warehouseService.createNewProductInWarehouse(request);
    }

    @Override
    public BookedProductsDto checkProductQuantity(@Valid ShoppingCartDto cart) {
        return warehouseService.checkProductQuantity(cart);
    }

    @Override
    public void addProductToWarehouse(@Valid AddProductToWarehouseRequest request) {
        warehouseService.addProductToWarehouse(request);
    }

    @Override
    public AddressDto getWarehouseAddress() {
        return warehouseService.getWarehouseAddress();
    }

    @Override
    public void shipToDelivery(ShippedToDeliveryRequest request) {
        warehouseService.shipToDelivery(request);
    }

    @Override
    public void acceptReturn(Map<UUID, Long> returnedProducts) {
        warehouseService.acceptReturn(returnedProducts);
    }

    @Override
    public BookedProductsDto assembleProducts(AssemblyProductsForOrderRequest request) {
        return warehouseService.assembleProducts(request);
    }
}
