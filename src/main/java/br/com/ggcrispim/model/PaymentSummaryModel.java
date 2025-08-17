package br.com.ggcrispim.model;

import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.dto.PaymentSummary;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Entity
@Table(name = "payment_summary")
public class PaymentSummaryModel {

    public PaymentSummaryModel() {}

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @Column(unique = true, nullable = false)
    private UUID correlationId;

    @Column
    private double amount;

    @Column(name="requested_at")
    private LocalDateTime requestedAt;

    @Column(name="payment_strategy")
    private PaymentStrategy paymentStrategy;

    public UUID getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(UUID correlationId) {
        this.correlationId = correlationId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public LocalDateTime getRequestedAt() {
        return requestedAt;
    }

    public void setRequestedAt(LocalDateTime requestedAt) {
        this.requestedAt = requestedAt;
    }

    public PaymentStrategy getPaymentStrategy() {
        return paymentStrategy;
    }

    public void setPaymentStrategy(PaymentStrategy paymentStrategy) {
        this.paymentStrategy = paymentStrategy;
    }

    public static Uni<List<PaymentSummaryModel>> findPaymentsByDateRange(Pool client, LocalDateTime startDate, LocalDateTime endDate) {
        return client
                .preparedQuery("SELECT * from payment_summary WHERE requested_at >= $1 AND requested_at <= $2")
                .execute(Tuple.of(startDate, endDate))
                .onItem()
                .transform(rows -> {
                    if (rows.rowCount() == 0) {
                        return List.of(); // Return empty list
                    }
                    return StreamSupport.stream(rows.spliterator(), false)
                            .map(row -> {
                                PaymentSummaryModel payment = new PaymentSummaryModel();
                                payment.setCorrelationId(row.getUUID("correlationid"));
                                payment.setAmount(row.getDouble("amount"));
                                payment.setRequestedAt(row.getLocalDateTime("requested_at"));
                                payment.setPaymentStrategy(PaymentStrategy.DEFAULT);
                                return payment;
                            })
                            .collect(Collectors.toList());
                });
    }

    public static Uni<Void> insertIntoDatabase(Pool client, PaymentRequest PaymentRequest) {
        return client
                .preparedQuery("INSERT INTO payment_summary (correlationid, amount, requested_at, payment_strategy) VALUES ($1, $2, $3, $4)")
                .execute(Tuple.of(PaymentRequest.getCorrelationId(), PaymentRequest.getAmount(), PaymentRequest.getRequestedAt(), PaymentStrategy.DEFAULT))
                .onFailure().invoke(failure -> {
                    // Log the error (optional)
                    System.err.println("Failed to insert payment summary: " + failure.getMessage());
                }).replaceWithVoid();
    }
}
