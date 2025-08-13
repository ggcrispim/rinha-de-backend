package br.com.ggcrispim.service;

import br.com.ggcrispim.config.RedisJobState;
import br.com.ggcrispim.dto.PaymentProcessorState;
import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.model.PaymentStrategy;
import br.com.ggcrispim.model.PaymentSummaryRepository;
import br.com.ggcrispim.producer.PaymentStreamProducer;
import br.com.ggcrispim.restclient.PaymentProcessorDefaultClient;
import br.com.ggcrispim.restclient.PaymentProcessorFallBackClient;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.redis.client.Command;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;

@ApplicationScoped
public class RedisQueueService {

    private final ReactiveRedisDataSource reactiveRedisClient;
    private final PaymentSummaryRepository paymentSummaryRepository;
    private final PaymentStreamProducer paymentStreamProducer;
    @RestClient
    private PaymentProcessorDefaultClient paymentProcessorDefaultClient;
    @RestClient
    PaymentProcessorFallBackClient fallbackClient;

    @Inject
    public RedisQueueService(ReactiveRedisDataSource reactiveRedisClient, PaymentSummaryRepository paymentSummaryRepository, PaymentStreamProducer paymentStreamProducer) {
        this.reactiveRedisClient = reactiveRedisClient;
        this.paymentSummaryRepository = paymentSummaryRepository;
        this.paymentStreamProducer = paymentStreamProducer;
    }

    private static final Logger LOG = Logger.getLogger(RedisQueueService.class);

    public Uni<Void> enqueueJob(PaymentRequest paymentRequest) {
        paymentRequest.setRequestedAt(LocalDateTime.now());
        return paymentStreamProducer.addPaymentToStream(paymentRequest)
                .onItem().call(x -> {
                    LOG.info("Job enqueued with correlationId: " + paymentRequest.getCorrelationId());
                    return Uni.createFrom().voidItem();
                })
                .onFailure().call(failure -> {
                    LOG.error("Failed to enqueue job", failure);
                    return Uni.createFrom().voidItem();
                })
                .replaceWithVoid();
    }

    // In your RedisQueueService, replace the Redis queue with stream
    public Uni<PaymentRequest> processPaymentWithHealthCheck(PaymentRequest paymentRequest) {
        return getServiceStateAndPersistInRedis()
                .onItem().transformToUni(state -> {
                    if (state.isFailing() || state.getMinResponseTime() > 1000) {
                        LOG.warn("Payment processor is failing, using fallback strategy for correlationId: " + paymentRequest.getCorrelationId());
                        paymentRequest.setPaymentStrategy(PaymentStrategy.FALLBACK);
                        return fallbackClient.processPayment(paymentRequest)
                                .onItem().transformToUni(response -> {
                                    LOG.info("Payment processed successfully with fallback for correlationId: " + paymentRequest.getCorrelationId());
                                    String updatedPaymentString = JsonObject.mapFrom(paymentRequest).toString();
                                    return Uni.createFrom().item(paymentRequest);
                                });

                    } else {
                        LOG.info("Payment processor is healthy, processing payment for correlationId: " + paymentRequest.getCorrelationId());
                        paymentRequest.setPaymentStrategy(PaymentStrategy.DEFAULT);
                        return paymentProcessorDefaultClient.processPayment(paymentRequest)
                                .onItem().transformToUni(response -> {
                                    LOG.info("Payment processed successfully with default processor for correlationId: " + paymentRequest.getCorrelationId());
                                    String updatedPaymentString = JsonObject.mapFrom(paymentRequest).toString();
                                    return Uni.createFrom().item(paymentRequest);
                                });
                    }
                }).onFailure().recoverWithUni(failure -> {;
                    LOG.error("Health check failed, using fallback strategy for correlationId: " + paymentRequest.getCorrelationId(), failure);
                    return fallbackClient.processPayment(paymentRequest)
                            .onItem().transformToUni(response -> {
                                LOG.info("Payment processed successfully with fallback after health check failure for correlationId: " + paymentRequest.getCorrelationId());
                                String failedPayment = JsonObject.mapFrom(paymentRequest).toString();
                                return Uni.createFrom().item(paymentRequest);
                            });
                });
    }


    public Uni<PaymentProcessorState> getServiceStateAndPersistInRedis() {
        return reactiveRedisClient.key().exists(RedisJobState.DEFAULT_PAYMENT_PROCESSOR_STATUS.getState())
                .onItem().transformToUni(exists -> {
                    if(exists) {
                        return reactiveRedisClient.execute(Command.GET, RedisJobState.DEFAULT_PAYMENT_PROCESSOR_STATUS.getState())
                                .onItem().transformToUni(response -> {
                                    if (response == null) {
                                        LOG.warn("No service health data found in Redis for default payment processor.");
                                        return Uni.createFrom().item(new PaymentProcessorState(false, 1));
                                    }
                                    PaymentProcessorState state = new JsonObject(response.toString()).mapTo(PaymentProcessorState.class);
                                    LOG.info("Service health data retrieved from Redis: " + state.isFailing());
                                    return Uni.createFrom().item(state);
                                });
                    } else {
                        return paymentProcessorDefaultClient.serviceHealth()
                                .onItem().transformToUni( state -> {
                                    String stateString = JsonObject.mapFrom(state).toString();
                                    return reactiveRedisClient.execute(Command.SETEX, RedisJobState.DEFAULT_PAYMENT_PROCESSOR_STATUS.getState(), "5", stateString)
                                            .onItem().transformToUni(x -> {
                                                LOG.info("Service health data persisted in Redis: " + stateString);
                                                return Uni.createFrom().item(state);
                                            });

                                }).onFailure().recoverWithItem(new PaymentProcessorState(false, 1));
                    }

                });
    }
}

