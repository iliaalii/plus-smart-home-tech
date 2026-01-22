package ru.yandex.practicum.dto.order;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto {
    @NotNull
    private UUID orderId;

    @NotBlank
    private UUID shoppingCartId;

    @NotEmpty
    private Map<UUID, Long> products;

    @NotBlank
    private UUID paymentId;

    @NotBlank
    private UUID deliveryId;

    private OrderState state;

    private Double deliveryWeight;

    private Double deliveryVolume;

    private Boolean fragile;

    private BigDecimal totalPrice;

    private BigDecimal deliveryPrice;

    private BigDecimal productPrice;
}
