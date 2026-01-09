package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "warehouse_products")
public class ProductInWarehouse {
    @Id
    @Column(name = "product_id", nullable = false, unique = true)
    private UUID productId;

    @Column(name = "fragile", nullable = false)
    private boolean fragile;

    @Embedded
    private Dimension dimension;

    @Column(name = "weight", nullable = false)
    private Double weight;

    @Column(name = "quantity")
    private long quantity;
}