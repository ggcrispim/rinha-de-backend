package br.com.ggcrispim.consumer;

import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.mapper.PaymentSummaryModelMapper;
import br.com.ggcrispim.model.PaymentSummaryModel;
import br.com.ggcrispim.service.RedisQueueService;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.XGroupCreateArgs;
import io.quarkus.redis.datasource.stream.XReadGroupArgs;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class PaymentStreamConsumer {

    private static final Logger LOG = Logger.getLogger(PaymentStreamConsumer.class);
    private static final String PAYMENT_STREAM = "payments:stream";
    private static final String CONSUMER_GROUP = "payment-processors";
    @ConfigProperty(name = "payment.stream.batch-size", defaultValue = "30")
    private int BATCH_SIZE;
    private static final String[] CONSUMER_NAMES = {"processor-1", "processor-2", "processor-3", "processor-4", "processor-5",
                                                    "processor-6", "processor-7", "processor-8", "processor-9", "processor-10"};

    private final AtomicInteger activeProcessingCount = new AtomicInteger(0);

    @Inject
    ReactiveRedisDataSource reactiveRedisClient;

    @Inject
    PaymentSummaryModelMapper paymentSummaryModelMapper;

    @Inject
    RedisQueueService redisQueueService;

    @Inject
    Pool client;

    @Scheduled(every = "1s")
    public Uni<Void> consumePaymentStream() {
        return Multi.createFrom().items(CONSUMER_NAMES)
                .onItem().transformToUniAndConcatenate(this::consumeWithConsumerName)
                .collect().asList()
                .onFailure().invoke(failure ->
                        LOG.error("Failed to consume from payment stream", failure))
                .onFailure().recoverWithItem(List.of())
                .replaceWithVoid();
    }

    private Uni<Void> consumeWithConsumerName(String consumerName) {
        return ensureStreamAndGroupExist()
                .onItem().transformToUni(ignored -> readFromStream(consumerName));
    }

    private Uni<Void> ensureStreamAndGroupExist() {
        return reactiveRedisClient.stream(String.class)
                .xgroupCreate(PAYMENT_STREAM, CONSUMER_GROUP, "0", new XGroupCreateArgs().mkstream())
                .onFailure().recoverWithUni(ignored -> Uni.createFrom().voidItem())
                .replaceWithVoid();
    }

    private Uni<Void> readFromStream(String consumerName) {
        XReadGroupArgs args = new XReadGroupArgs()
                .count(BATCH_SIZE)
                .block(Duration.ofMillis(10));

        return reactiveRedisClient.stream(String.class)
                .xreadgroup(CONSUMER_GROUP, consumerName, PAYMENT_STREAM, ">", args)
                .onItem().transformToUni(this::processStreamMessagesReactively)
                .onFailure().invoke(failure -> {
                    if (!failure.getMessage().contains("NOGROUP")) {
                        LOG.error("Error reading from stream with consumer: " + consumerName, failure);
                    }
                })
                .onFailure().recoverWithUni(ignored -> Uni.createFrom().voidItem());
    }

    private Uni<Void> processStreamMessagesReactively(List<StreamMessage<String, String, String>> messages) {
        return filterAndLogMessages(messages)
                .onItem().transformToUni(this::processValidMessages);
    }

    private Uni<List<StreamMessage<String, String, String>>> filterAndLogMessages(
            List<StreamMessage<String, String, String>> messages) {

        if (messages == null || messages.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }

        // Reactive filtering without worker threads
        List<StreamMessage<String, String, String>> validMessages = new ArrayList<>();
        List<String> initIds = new ArrayList<>();

        for (StreamMessage<String, String, String> message : messages) {
            if (message.payload().containsKey("init")) {
                initIds.add(message.id());
            } else {
                validMessages.add(message);
            }
        }

        if (!initIds.isEmpty()) {
            return acknowledgeMessages(initIds)
                    .onItem().transform(ignored -> validMessages);
        }

        return Uni.createFrom().item(validMessages);
    }

    private Uni<Void> processValidMessages(List<StreamMessage<String, String, String>> validMessages) {
        if (validMessages.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        LOG.info("Processing " + validMessages.size() + " messages from stream");

        // Apply backpressure with controlled concurrency - fully reactive
        return Multi.createFrom().iterable(validMessages)
                .onItem().transformToUni(this::processStreamMessageWithBackpressure)
                .merge(25)
                .collect().asList()
                .onItem().transformToUni(successfulMessageIds -> {
                    LOG.info("Successfully processed " + successfulMessageIds.size() + " out of " + validMessages.size() + " messages");
                    return acknowledgeMessages(successfulMessageIds);
                })
                .onItem().invoke(count ->
                        LOG.info("Successfully acknowledged " + count + " messages"))
                .replaceWithVoid();
    }

    private Uni<String> processStreamMessageWithBackpressure(StreamMessage<String, String, String> message) {
        // Remove the skipping logic - process all messages
        activeProcessingCount.incrementAndGet();
        return processStreamMessageReactively(message)
                .onTermination().invoke(() -> activeProcessingCount.decrementAndGet());
    }

    private Uni<String> processStreamMessageReactively(StreamMessage<String, String, String> message) {
        return extractPayloadFromMessage(message)
                .onItem().transformToUni(payloadJson ->
                        parsePaymentRequestReactively(payloadJson, message.id()))
                .onItem().transformToUni(paymentRequest ->
                        processPaymentAndPersist(paymentRequest, message.id()))
                .onFailure().invoke(failure ->
                        LOG.error("Failed to process stream message: " + message.id(), failure));

    }

    private Uni<String> extractPayloadFromMessage(StreamMessage<String, String, String> message) {
        Map<String, String> fields = message.payload();
        String payloadJson = fields.get("payload");

        if (payloadJson == null) {
            return Uni.createFrom().failure(
                    new IllegalArgumentException("No payload found in message: " + message.id()));
        }

        return Uni.createFrom().item(payloadJson);
    }

    private Uni<PaymentRequest> parsePaymentRequestReactively(String payloadJson, String messageId) {
        try {
            PaymentRequest paymentRequest = new JsonObject(payloadJson).mapTo(PaymentRequest.class);
            return Uni.createFrom().item(paymentRequest);
        } catch (Exception e) {
            return Uni.createFrom().failure(
                    new RuntimeException("Failed to parse payment request for message: " + messageId, e));
        }
    }

    public Uni<String> processPaymentAndPersist(PaymentRequest paymentRequest, String messageId) {
        return redisQueueService.processPaymentWithHealthCheck(paymentRequest)
                .onItem().transformToUni(processedPayment -> persistPaymentSummary(paymentRequest))
                .onItem().invoke(saved ->
                        LOG.debug("Processed and persisted payment from stream: " + paymentRequest.getCorrelationId()))
                .onItem().transform(ignored -> messageId)
                .onFailure().invoke(failure -> handleProcessingFailure(failure, paymentRequest))
                .onFailure().recoverWithItem(messageId);
    }

    public Uni<String> processPaymentAndPersist(PaymentRequest paymentRequest) {
        return redisQueueService.processPaymentWithHealthCheck(paymentRequest)
                .onItem().transformToUni(processedPayment -> persistPaymentSummary(paymentRequest))
                .onItem().invoke(saved ->
                        LOG.debug("Processed and persisted payment from stream: " + paymentRequest.getCorrelationId()))
                .onItem().transform(ignored -> paymentRequest.getCorrelationId())
                .onFailure().invoke(failure -> handleProcessingFailure(failure, paymentRequest))
                .onFailure().recoverWithItem(paymentRequest.getCorrelationId());
    }

    private Uni<PaymentRequest> persistPaymentSummary(PaymentRequest paymentRequest) {
        return PaymentSummaryModel.insertIntoDatabase(client, paymentRequest)
                .onItem().invoke(() ->
                        LOG.debug("Payment summary persisted: " + paymentRequest.getCorrelationId()))
                .onFailure().invoke(failure ->
                        LOG.error("Failed to persist payment summary: " + paymentRequest.getCorrelationId(), failure))
                .replaceWith(paymentRequest);
    }

    private void handleProcessingFailure(Throwable failure, PaymentRequest paymentRequest) {
        if (failure.getMessage() != null && failure.getMessage().contains("duplicate key")) {
            LOG.warn("Duplicate payment ignored: " + paymentRequest.getCorrelationId());
        } else {
            LOG.error("Failed to process/persist payment: " + paymentRequest.getCorrelationId(), failure);
        }
    }

    private Uni<Integer> acknowledgeMessages(List<String> messageIds) {
        if (messageIds.isEmpty()) {
            return Uni.createFrom().item(0);
        }

        return reactiveRedisClient.stream(String.class)
                .xack(PAYMENT_STREAM, CONSUMER_GROUP, messageIds.toArray(new String[0]))
                .onItem().invoke(count ->
                        LOG.debug("Acknowledged " + count + " messages"));
    }
}