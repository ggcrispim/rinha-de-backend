package br.com.ggcrispim.producer;

import br.com.ggcrispim.dto.PaymentRequest;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;

@ApplicationScoped
public class PaymentStreamProducer {

    private static final Logger LOG = Logger.getLogger(PaymentStreamProducer.class);
    private static final String PAYMENT_STREAM = "payments:stream";

    @Inject
    ReactiveRedisDataSource reactiveRedisClient;

    public Uni<Void> addPaymentToStream(PaymentRequest paymentRequest) {
        return addSinglePaymentToStream(paymentRequest)
                .onItem().invoke(streamId ->
                        LOG.debug("Added payment to stream with ID: " + streamId))
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