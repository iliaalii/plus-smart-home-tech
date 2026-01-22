package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.dto.payment.PaymentState;

import java.math.BigDecimal;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "payments")
public class Payment {
    @Id
    @Column(name = "payment_id")
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID paymentId;

    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "product_price")
    private BigDecimal productPrice;

    @Column(name = "total_payment")
    private BigDecimal totalPayment;

    @Column(name = "delivery_total")
    private BigDecimal deliveryTotal;

    @Column(name = "fee_total")
    private BigDecimal feeTotal;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_state")
    private PaymentState paymentStatus;
}
