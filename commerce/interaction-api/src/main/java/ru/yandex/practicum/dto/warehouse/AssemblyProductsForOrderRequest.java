package ru.yandex.practicum.dto.warehouse;

import lombok.*;

import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AssemblyProductsForOrderRequest {
    private UUID orderId;

    private Map<UUID, Long> products;
}
