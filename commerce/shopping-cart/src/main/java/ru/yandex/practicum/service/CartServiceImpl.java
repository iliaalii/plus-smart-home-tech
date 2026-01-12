package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.feign.WarehouseClient;
import ru.yandex.practicum.mapper.ShoppingCartMapper;
import ru.yandex.practicum.model.ShoppingCart;
import ru.yandex.practicum.repository.ShoppingCartRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RequiredArgsConstructor
@Service
@Slf4j
public class CartServiceImpl implements CartService {
    private final ShoppingCartRepository cartRepository;
    private final WarehouseClient warehouseClient;
    private final ShoppingCartMapper mapper;

    @Override
    @Transactional(readOnly = true)
    public ShoppingCartDto getCart(String username) {
        log.info("Получение корзины пользователя: {}", username);
        return cartRepository.findByUsernameAndIsActive(username, true)
                .map(mapper::toDto)
                .orElseGet(() -> mapper.toDto(createNewCart(username)));
    }

    @Override
    @Transactional
    public ShoppingCartDto addProductToCart(String username, Map<UUID, Long> products) {
        log.info("Добавление продуктов в корзину пользователя: {}", username);
        ShoppingCart cart = cartRepository.findByUsernameAndIsActive(username, true)
                .orElseGet(() -> createNewCart(username));

        if (cart.getProducts() == null) {
            cart.setProducts(new HashMap<>());
        }

        products.forEach((productId, quantity) ->
                cart.getProducts().merge(productId, quantity, Long::sum)
        );

        log.info("Отправляем корзину {} для проверки доступности товара на складе", cart);
        warehouseClient.checkProductQuantity(mapper.toDto(cart));

        return mapper.toDto(cartRepository.save(cart));
    }


    @Override
    @Transactional
    public void deleteCart(String username) {
        log.info("Деактивация корзины пользователя: {}", username);
        cartRepository.findByUsernameAndIsActive(username, true)
                .ifPresent(cart -> {
                    cart.setIsActive(false);
                    cartRepository.save(cart);
                });
    }

    @Override
    @Transactional
    public ShoppingCartDto removeFromCart(String username, Set<UUID> productIds) {
        log.info("Удаление продуктов из корзины пользователя: {}", username);
        ShoppingCart cart = cartRepository.findByUsernameAndIsActive(username, true)
                .orElseThrow(() -> new NoProductsInShoppingCartException("Корзина пользователя не найдена"));

        if (!cart.getProducts().keySet().containsAll(productIds)) {
            throw new NoProductsInShoppingCartException("В корзине нет одного или нескольких товаров");
        }

        cart.getProducts().keySet().removeAll(productIds);

        return mapper.toDto(cartRepository.save(cart));
    }


    @Override
    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        log.info("Изменение количества продукта в корзине пользователя: {}", username);
        ShoppingCart cart = cartRepository.findByUsernameAndIsActive(username, true)
                .orElseThrow(() -> new NoProductsInShoppingCartException(
                        "Корзина пользователя" + username + " не найдена"));

        UUID productId = request.getProductId();

        if (!cart.getProducts().containsKey(productId)) {
            throw new NoProductsInShoppingCartException(
                    "Товар = " + productId + " не найден в корзине"
            );
        }

        cart.getProducts().put(productId, request.getNewQuantity());

        return mapper.toDto(cartRepository.save(cart));
    }

    private ShoppingCart createNewCart(String username) {
        log.info("Создание новой корзины");
        return cartRepository.save(ShoppingCart.builder()
                .username(username)
                .isActive(true)
                .build());
    }
}
