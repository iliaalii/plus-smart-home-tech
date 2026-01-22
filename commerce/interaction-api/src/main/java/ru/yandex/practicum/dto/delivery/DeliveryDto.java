package ru.yandex.practicum.dto.delivery;

import jakarta.validation.constraints.NotBlank;
import lombok.*;
import ru.yandex.practicum.dto.warehouse.AddressDto;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeliveryDto {
    private UUID deliveryId;

    @NotBlank
    private AddressDto fromAddress;

    @NotBlank
    private AddressDto toAddress;

    @NotBlank
    private UUID orderId;

    @NotBlank
    private DeliveryState deliveryState;
}
