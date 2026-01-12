package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface CartService {
    ShoppingCartDto getCart(String username);

    ShoppingCartDto addProductToCart(String username, Map<UUID, Long> products);

    void deleteCart(String username);

    ShoppingCartDto removeFromCart(String username, Set<UUID> productIds);

    ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request);
}
