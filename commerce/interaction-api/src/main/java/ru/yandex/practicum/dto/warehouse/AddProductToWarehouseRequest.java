package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AddProductToWarehouseRequest {
    @NotNull
    private UUID productId;

    @Min(value = 1)
    @NotNull
    private Long quantity;
}
