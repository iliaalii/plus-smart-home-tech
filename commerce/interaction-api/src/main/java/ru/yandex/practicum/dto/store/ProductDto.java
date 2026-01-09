package ru.yandex.practicum.dto.store;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProductDto {
    private UUID productId;

    @NotBlank
    @Size(max = 500)
    private String productName;

    @NotBlank
    @Size(max = 2000)
    private String description;

    @Size(max = 2048)
    private String imageSrc;

    @NotNull
    private QuantityState quantityState;

    @NotNull
    private ProductState productState;

    private ProductCategory productCategory;

    @NotNull
    @DecimalMin(value = "1.0")
    private Float price;
}
