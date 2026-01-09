package ru.yandex.practicum.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.dto.store.ProductCategory;
import ru.yandex.practicum.dto.store.QuantityState;

import java.util.UUID;

public interface StoreService {
    Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable);

    ProductDto addProduct(ProductDto productDto);

    ProductDto updateProduct(ProductDto productDto);

    Boolean removeProduct(UUID productId);

    Boolean updateQuantityState(UUID productId, QuantityState quantityState);

    ProductDto getProductById(UUID productId);
}