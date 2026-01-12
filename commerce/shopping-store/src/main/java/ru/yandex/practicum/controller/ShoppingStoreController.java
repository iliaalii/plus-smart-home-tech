package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.dto.store.ProductCategory;
import ru.yandex.practicum.dto.store.QuantityState;
import ru.yandex.practicum.feign.ShoppingStoreClient;
import ru.yandex.practicum.service.StoreService;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-store")
public class ShoppingStoreController implements ShoppingStoreClient {
    private final StoreService storeService;

    @Override
    public Page<ProductDto> getProductsByCategory(ProductCategory category, @Valid Pageable pageable) {
        return storeService.getProductsByCategory(category, pageable);
    }

    @Override
    public ProductDto addProduct(@Valid ProductDto productDto) {
        return storeService.addProduct(productDto);
    }

    @Override
    public ProductDto updateProduct(@Valid ProductDto productDto) {
        return storeService.updateProduct(productDto);
    }

    @Override
    public Boolean removeProduct(UUID productId) {
        return storeService.removeProduct(productId);
    }

    @Override
    public Boolean updateQuantityState(UUID productId, QuantityState quantityState) {
        return storeService.updateQuantityState(productId, quantityState);
    }

    @Override
    public ProductDto getProductById(UUID productId) {
        return storeService.getProductById(productId);
    }
}
