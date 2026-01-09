package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.dto.store.ProductCategory;
import ru.yandex.practicum.dto.store.ProductState;
import ru.yandex.practicum.dto.store.QuantityState;

import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "products")
public class Product {
    @Id
    @Column(name = "product_id")
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID productId;

    @Column(name = "product_name", length = 500, nullable = false)
    private String productName;

    @Column(name = "description", length = 2000, nullable = false)
    private String description;

    @Column(name = "image_src", length = 2024)
    private String imageSrc;

    @Enumerated(EnumType.STRING)
    @Column(name = "quantity_state", length = 20, nullable = false)
    private QuantityState quantityState;

    @Enumerated(EnumType.STRING)
    @Column(name = "product_state", length = 20, nullable = false)
    private ProductState productState;

    @Enumerated(EnumType.STRING)
    @Column(name = "product_category", length = 20)
    private ProductCategory productCategory;

    @Column(name = "price")
    private Float price;
}
