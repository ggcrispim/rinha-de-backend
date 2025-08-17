package br.com.ggcrispim.service;

import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.model.PaymentStrategy;
import br.com.ggcrispim.restclient.PaymentProcessorDefaultClient;
import br.com.ggcrispim.restclient.PaymentProcessorFallBackClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.time.temporal.ChronoUnit;

@ApplicationScoped
public class PaymentService {

    @RestClient
    PaymentProcessorDefaultClient defaultClient;

    @RestClient
    PaymentProcessorFallBackClient fallbackClient;

    private static final Logger LOG = Logger.getLogger(PaymentService.class);

    @Retry(maxRetries = -1, delay = 5, delayUnit = ChronoUnit.SECONDS, jitter = 50)
    //@Fallback(fallbackMethod = "processWithFallback")
    @Timeout(value = 5, unit = ChronoUnit.SECONDS)
    public Uni<Response> processPayment(PaymentRequest paymentRequest) {
        LOG.info("Processing payment for correlationId: " + paymentRequest.getCorrelationId());
        paymentRequest.setPaymentStrategy(PaymentStrategy.DEFAULT);

        return defaultClient.processPayment(paymentRequest)
                .onItem().invoke(() ->
                        LOG.info("Payment processed successfully for correlationId: " + paymentRequest.getCorrelationId()));
    }

    public Uni<Response> processWithFallback(PaymentRequest paymentRequest) {
        LOG.warn("Using fallback for correlationId: " + paymentRequest.getCorrelationId());
        paymentRequest.setPaymentStrategy(PaymentStrategy.FALLBACK);

        return fallbackClient.processPayment(paymentRequest)
                .onItem().invoke(() ->
                        LOG.info("Payment processed with fallback for correlationId: " + paymentRequest.getCorrelationId()));
    }
}