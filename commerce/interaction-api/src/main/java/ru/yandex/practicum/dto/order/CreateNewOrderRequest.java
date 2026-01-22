package ru.yandex.practicum.dto.order;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CreateNewOrderRequest {
    @NotBlank
    private ShoppingCartDto shoppingCart;

    @NotBlank
    private AddressDto deliveryAddress;

    @NotNull
    private String username;
}
