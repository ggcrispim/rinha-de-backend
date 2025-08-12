package br.com.ggcrispim.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PaymentSummary {

    @JsonProperty(value = "default")
    private Summary defaultPayment;
    @JsonProperty(value = "fallback")
    private Summary fallbackPayment;

    public PaymentSummary(Summary defaultPayment, Summary fallbackPayment) {
        this.defaultPayment = defaultPayment;
        this.fallbackPayment = fallbackPayment;
    }

    public Summary getDefaultPayment() {
        return defaultPayment;
    }

    public void setDefaultPayment(Summary defaultPayment) {
        this.defaultPayment = defaultPayment;
    }

    public Summary getFallbackPayment() {
        return fallbackPayment;
    }

    public void setFallbackPayment(Summary fallbackPayment) {
        this.fallbackPayment = fallbackPayment;
    }
}
