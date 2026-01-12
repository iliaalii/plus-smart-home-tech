package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.feign.ShoppingCartClient;
import ru.yandex.practicum.service.CartService;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-cart")
@Slf4j
public class ShoppingCartController implements ShoppingCartClient {
    private final CartService cartService;

    @Override
    public ShoppingCartDto getCart(String username) {
        checkUser(username);
        return cartService.getCart(username);
    }

    @Override
    public ShoppingCartDto addProductToCart(String username, Map<UUID, Long> products) {
        checkUser(username);
        validateProductsMap(products);
        return cartService.addProductToCart(username, products);
    }

    @Override
    public void deleteCart(String username) {
        checkUser(username);
        cartService.deleteCart(username);
    }

    @Override
    public ShoppingCartDto removeFromCart(String username, Set<UUID> productIds) {
        checkUser(username);
        validateProductIds(productIds);
        return cartService.removeFromCart(username, productIds);
    }

    @Override
    public ShoppingCartDto changeProductQuantity(String username, @Valid ChangeProductQuantityRequest request) {
        checkUser(username);
        return cartService.changeProductQuantity(username, request);
    }

    private void checkUser(String username) {
        log.info("Проверка авторизации пользователем");
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Пользователь не авторизован");
        }
    }

    private void validateProductsMap(Map<UUID, Long> products) {
        log.info("Валидации списка продукции");
        if (products == null || products.isEmpty()) {
            throw new IllegalArgumentException("Список товаров не должен быть пустым");
        }

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("productId не должен быть null");
            }
            if (entry.getValue() == null || entry.getValue() < 1) {
                throw new IllegalArgumentException(
                        "Количество товара должно быть больше 0"
                );
            }
        }
    }

    private void validateProductIds(Set<UUID> productIds) {
        log.info("Валидация списка id продукции");
        if (productIds == null || productIds.isEmpty()) {
            throw new IllegalArgumentException("Список productIds не должен быть пустым");
        }

        if (productIds.contains(null)) {
            throw new IllegalArgumentException("productId не должен быть null");
        }
    }
}
