package br.com.ggcrispim.consumer;

import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.mapper.PaymentSummaryModelMapper;
import br.com.ggcrispim.model.PaymentSummaryRepository;
import br.com.ggcrispim.service.RedisQueueService;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.XGroupCreateArgs;
import io.quarkus.redis.datasource.stream.XReadGroupArgs;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class PaymentStreamConsumer {

    private static final Logger LOG = Logger.getLogger(PaymentStreamConsumer.class);
    private static final String PAYMENT_STREAM = "payments:stream";
    private static final String CONSUMER_GROUP = "payment-processors";
    private static final String CONSUMER_NAME = "processor-1";
    private static final int BATCH_SIZE = 1000;

    @Inject
    ReactiveRedisDataSource reactiveRedisClient;

    @Inject
    PaymentSummaryRepository paymentSummaryRepository;

    @Inject
    PaymentSummaryModelMapper paymentSummaryModelMapper;

    @Inject
    RedisQueueService redisQueueService;

    @Scheduled(every = "1s")
    @WithTransaction
    public Uni<Void> consumePaymentStream() {
        return ensureStreamAndGroupExist()
                .onItem().transformToUni(ignored -> readFromStream())
                .onFailure().invoke(failure ->
                        LOG.error("Failed to consume from payment stream", failure))
                .onFailure().recoverWithUni(ignored -> Uni.createFrom().voidItem());
    }

    private Uni<Void> ensureStreamAndGroupExist() {
        return createConsumerGroup();
    }

    private Uni<Void> createConsumerGroup() {
        return reactiveRedisClient.stream(String.class)
                .xgroupCreate(PAYMENT_STREAM, CONSUMER_GROUP, "0", new XGroupCreateArgs().mkstream())
                .onItem().invoke(ignored ->
                        LOG.info("Consumer group created or already exists: " + CONSUMER_GROUP))
                .onFailure().invoke(failure -> {
                    if (failure.getMessage().contains("BUSYGROUP")) {
                        LOG.debug("Consumer group already exists: " + CONSUMER_GROUP);
                    } else {
                        LOG.warn("Failed to create consumer group", failure);
                    }
                })
                .onFailure().recoverWithUni(ignored -> Uni.createFrom().voidItem())
                .replaceWithVoid();
    }

    private Uni<Void> readFromStream() {
        XReadGroupArgs args = new XReadGroupArgs()
                .count(BATCH_SIZE)
                .block(Duration.ofMillis(100));

        return reactiveRedisClient.stream(String.class)
                .xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, PAYMENT_STREAM, ">", args)
                .onItem().transformToUni(this::processStreamMessages)
                .onFailure().invoke(failure -> {
                    if (!failure.getMessage().contains("NOGROUP")) {
                        LOG.error("Error reading from stream", failure);
                    }
                })
                .onFailure().recoverWithUni(ignored -> Uni.createFrom().voidItem());
    }

    private Uni<Void> processStreamMessages(List<StreamMessage<String, String, String>> messages) {
        if (messages == null || messages.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        // Filter out initialization messages
        List<StreamMessage<String, String, String>> validMessages = messages.stream()
                .filter(msg -> !msg.payload().containsKey("init"))
                .toList();

        if (validMessages.isEmpty()) {
            // Acknowledge init messages without processing
            List<String> initIds = messages.stream()
                    .filter(msg -> msg.payload().containsKey("init"))
                    .map(StreamMessage::id)
                    .toList();

            if (!initIds.isEmpty()) {
                return acknowledgeMessages(initIds).replaceWithVoid();
            }
            return Uni.createFrom().voidItem();
        }

        LOG.info("Processing " + validMessages.size() + " messages from stream");

        List<Uni<String>> processOps = validMessages.stream()
                .map(this::processStreamMessage)
                .toList();

        return Uni.join().all(processOps).andFailFast()
                .onItem().transformToUni(messageIds -> acknowledgeMessages(messageIds))
                .onItem().invoke(count ->
                        LOG.info("Successfully processed and acknowledged " + count + " messages"))
                .replaceWithVoid();
    }

    private Uni<String> processStreamMessage(StreamMessage<String, String, String> message) {
        try {
            String messageId = message.id();
            Map<String, String> fields = message.payload();

            String payloadJson = fields.get("payload");
            if (payloadJson == null) {
                LOG.warn("No payload found in message: " + messageId);
                return Uni.createFrom().item(messageId);
            }

            PaymentRequest paymentRequest = new JsonObject(payloadJson).mapTo(PaymentRequest.class);

            // Process payment with health check before persisting
            return redisQueueService.processPaymentWithHealthCheck(paymentRequest)
                    .onItem().transformToUni(processedPayment -> {
                        var paymentSummary = paymentSummaryModelMapper.map(processedPayment);
                        return paymentSummaryRepository.persist(paymentSummary)
                                .onItem().invoke(saved ->
                                        LOG.debug("Processed and persisted payment from stream: " + saved.getCorrelationId()))
                                .onItem().transform(ignored -> messageId);
                    })
                    .onFailure().invoke(failure -> {
                        if (failure.getMessage().contains("duplicate key")) {
                            LOG.warn("Duplicate payment ignored: " + paymentRequest.getCorrelationId());
                        } else {
                            LOG.error("Failed to process/persist payment: " + paymentRequest.getCorrelationId(), failure);
                        }
                    })
                    .onFailure().recoverWithItem(messageId);
        } catch (Exception e) {
            LOG.error("Failed to process stream message", e);
            return Uni.createFrom().item(message.id());
        }
    }

    private Uni<Integer> acknowledgeMessages(List<String> messageIds) {
        if (messageIds.isEmpty()) {
            return Uni.createFrom().item(0);
        }

        String[] ids = messageIds.toArray(new String[0]);

        return reactiveRedisClient.stream(String.class)
                .xack(PAYMENT_STREAM, CONSUMER_GROUP, ids)
                .onItem().invoke(count ->
                        LOG.debug("Acknowledged " + count + " messages"));
    }
}