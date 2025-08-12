package br.com.ggcrispim.dto;

public class PaymentProcessorState {


    private boolean failing;
    private int minResponseTime;

    public PaymentProcessorState(){}

    public PaymentProcessorState(boolean failing, int minResponseTime) {
        this.failing = failing;
        this.minResponseTime = minResponseTime;
    }

    public boolean isFailing() {
        return failing;
    }

    public void setFailing(boolean failing) {
        this.failing = failing;
    }

    public int getMinResponseTime() {
        return minResponseTime;
    }

    public void setMinResponseTime(int minResponseTime) {
        this.minResponseTime = minResponseTime;
    }
}
