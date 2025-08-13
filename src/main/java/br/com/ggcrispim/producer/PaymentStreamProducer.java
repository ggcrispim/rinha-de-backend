package br.com.ggcrispim.producer;

import br.com.ggcrispim.dto.PaymentRequest;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class PaymentStreamProducer {

    private static final Logger LOG = Logger.getLogger(PaymentStreamProducer.class);
    private static final String PAYMENT_STREAM = "payments:stream";
    private static final int BATCH_SIZE = 1000;

    @Inject
    ReactiveRedisDataSource reactiveRedisClient;

    private final ConcurrentLinkedQueue<PaymentRequest> paymentBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger bufferCount = new AtomicInteger(0);

    public Uni<Void> addPaymentToStream(PaymentRequest paymentRequest) {
        paymentBuffer.offer(paymentRequest);
        int currentCount = bufferCount.incrementAndGet();

        if (currentCount >= BATCH_SIZE) {
            return flushPaymentsToStream();
        }

        return Uni.createFrom().voidItem();
    }

    public Uni<Void> flushPaymentsToStream() {
        if (paymentBuffer.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        List<PaymentRequest> paymentsToProcess = new ArrayList<>();
        PaymentRequest payment;

        // Extract payments from buffer
        while ((payment = paymentBuffer.poll()) != null && paymentsToProcess.size() < BATCH_SIZE) {
            paymentsToProcess.add(payment);
        }

        if (paymentsToProcess.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        final int paymentCount = paymentsToProcess.size();
        bufferCount.addAndGet(-paymentCount);

        LOG.info("Streaming " + paymentCount + " payments to Redis Stream");

        // Process payments sequentially for now - can be optimized later
        List<Uni<String>> streamOps = paymentsToProcess.stream()
                .map(this::addSinglePaymentToStream)
                .toList();

        return Uni.join().all(streamOps).andFailFast()
                .onItem().invoke(results ->
                        LOG.info("Successfully streamed " + paymentCount + " payments"))
                .replaceWithVoid();
    }

    private Uni<String> addSinglePaymentToStream(PaymentRequest paymentRequest) {
        Map<String, String> fields = Map.of(
                "correlationId", paymentRequest.getCorrelationId(),
                "amount", String.valueOf(paymentRequest.getAmount()),
                "requestedAt", paymentRequest.getRequestedAt().toString(),
                "payload", JsonObject.mapFrom(paymentRequest).encode()
        );

        return reactiveRedisClient.stream(String.class)
                .xadd(PAYMENT_STREAM, fields)
                .onItem().invoke(streamId ->
                        LOG.debug("Added payment to stream with ID: " + streamId))
                .onFailure().invoke(failure ->
                        LOG.error("Failed to add payment to stream: " + paymentRequest.getCorrelationId(), failure));
    }
}