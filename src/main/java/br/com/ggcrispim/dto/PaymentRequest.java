package br.com.ggcrispim.dto;

import br.com.ggcrispim.model.PaymentStrategy;
import java.time.LocalDateTime;

public class PaymentRequest {

    private String correlationId;
    private double amount;
    private LocalDateTime requestedAt;
    private PaymentStrategy paymentStrategy;

    public PaymentRequest(String correlationId, double amount, LocalDateTime requestedAt) {
        this.correlationId = correlationId;
        this.amount = amount;
        this.requestedAt = requestedAt;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
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

    public void setPaymentStrategy(PaymentStrategy paymentStrategy) {
        this.paymentStrategy = paymentStrategy;
    }

    public PaymentStrategy getPaymentStrategy() {
        return paymentStrategy;
    }
}
