package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BookedProductsDto {
    @NotNull
    private double deliveryWeight;

    @NotNull
    private double deliveryVolume;

    @NotNull
    private boolean fragile;
}
