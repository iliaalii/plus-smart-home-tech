package ru.yandex.practicum.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.dto.store.ProductCategory;
import ru.yandex.practicum.dto.store.QuantityState;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@FeignClient(name = "shopping-store", path = "/api/v1/shopping-store")
public interface ShoppingStoreClient {
    @GetMapping
    Page<ProductDto> getProductsByCategory(@RequestParam ProductCategory category, Pageable pageable);

    @PutMapping
    ProductDto addProduct(@RequestBody ProductDto productDto);

    @PostMapping
    ProductDto updateProduct(@RequestBody ProductDto productDto);

    @PostMapping("/removeProductFromStore")
    Boolean removeProduct(@RequestBody UUID productId);

    @PostMapping("/quantityState")
    Boolean updateQuantityState(@RequestParam UUID productId,
                                @RequestParam QuantityState quantityState);

    @GetMapping("/{productId}")
    ProductDto getProductById(@PathVariable UUID productId);

    @PostMapping("/products/prices")
    Map<UUID, BigDecimal> getPrices(@RequestBody Set<UUID> productIds);
}
