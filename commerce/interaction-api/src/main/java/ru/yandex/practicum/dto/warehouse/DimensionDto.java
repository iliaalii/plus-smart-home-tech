package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DimensionDto {
    @DecimalMin(value = "1.0", inclusive = true)
    @NotNull
    private Double width;

    @DecimalMin(value = "1.0", inclusive = true)
    @NotNull
    private Double height;

    @DecimalMin(value = "1.0", inclusive = true)
    @NotNull
    private Double depth;
}
