package br.com.ggcrispim.dto;

import java.math.BigDecimal;

public class Summary {

    private int totalRequests;
    private double totalAmount;

    public Summary(int totalRequests, double totalAmount) {
        this.totalRequests = totalRequests;
        this.totalAmount = totalAmount;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public void setTotalRequests(int totalRequests) {
        this.totalRequests = totalRequests;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }
}
