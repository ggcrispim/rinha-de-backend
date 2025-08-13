package br.com.ggcrispim.config;


public enum RedisJobState {
    PAYMENT_REQUESTED("payment_requested"),
    COMPLETED_JOBS_KEY("completed_payment_requests"),
    DEFAULT_PAYMENT_PROCESSOR_STATUS("default_payment_processor_status");

    private final String state;

    RedisJobState(String state) {
        this.state = state;
    }

   public String getState() {
        return this.state;
   }
}
