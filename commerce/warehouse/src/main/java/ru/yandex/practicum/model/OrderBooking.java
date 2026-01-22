package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "order_booking")
public class OrderBooking {
    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "delivery_id")
    private UUID deliveryId;

    @CollectionTable(name = "booking_products", joinColumns = @JoinColumn(name = "order_booking_id"))
    @ElementCollection
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Long> products;
}
