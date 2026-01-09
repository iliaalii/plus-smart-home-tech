package ru.yandex.practicum.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

@FeignClient(name = "shopping-cart", path = "/api/v1/shopping-cart")
public interface ShoppingCartClient {
    @GetMapping
    ShoppingCartDto getCart(@RequestParam String username);

    @PutMapping
    ShoppingCartDto addProductToCart(@RequestParam String username,
                                     @RequestBody Map<UUID, Long> products);

    @DeleteMapping
    void deleteCart(@RequestParam String username);

    @PostMapping("/remove")
    ShoppingCartDto removeFromCart(@RequestParam String username,
                                   @RequestBody Set<UUID> productIds);

    @PostMapping("/change-quantity")
    ShoppingCartDto changeProductQuantity(@RequestParam String username,
                                          @RequestBody ChangeProductQuantityRequest request);
}
