package br.com.ggcrispim.service;

import br.com.ggcrispim.config.RedisJobState;
import br.com.ggcrispim.dto.PaymentProcessorState;
import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.model.PaymentStrategy;
import br.com.ggcrispim.restclient.PaymentProcessorDefaultClient;
import br.com.ggcrispim.restclient.PaymentProcessorFallBackClient;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.redis.client.Command;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;
import java.util.UUID;

@ApplicationScoped
public class RedisQueueService {

    private final ReactiveRedisDataSource reactiveRedisClient;
    @RestClient
    private PaymentProcessorDefaultClient paymentProcessorDefaultClient;
    @RestClient
    PaymentProcessorFallBackClient fallbackClient;

    @Inject
    public RedisQueueService(ReactiveRedisDataSource reactiveRedisClient) {
        this.reactiveRedisClient = reactiveRedisClient;
    }

    private static final Logger LOG = Logger.getLogger(RedisQueueService.class);

    public Uni<Void> enqueueJob(PaymentRequest paymentRequest) {
        paymentRequest.setRequestedAt(LocalDateTime.now());
        paymentRequest.setCorrelationId(UUID.randomUUID().toString());
        String paymentString = JsonObject.mapFrom(paymentRequest).toString();
        return reactiveRedisClient.execute(Command.LPUSH, RedisJobState.PAYMENT_REQUESTED.getState(), paymentString)
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

    @Scheduled(every = "1s")
    void dequeueAndProcessJob() {
        reactiveRedisClient.execute(Command.RPOP, RedisJobState.PAYMENT_REQUESTED.getState())
                .onItem().ifNotNull().transformToUni(response -> {
                    String paymentString = response.toString();
                    PaymentRequest paymentRequest = new JsonObject(paymentString).mapTo(PaymentRequest.class);
                    LOG.info("Job dequeued with correlationId: " + paymentRequest.getCorrelationId());
                    return processPaymentWithHealthCheck(paymentRequest, paymentString);
                }) .subscribe().with(
                        success -> LOG.info("Job processed successfully"),
                        failure -> LOG.error("Error processing job")
                );
    }

    private Uni<Void> processPaymentWithHealthCheck(PaymentRequest paymentRequest, String paymentString) {
        return getServiceStateAndPersistInRedis()
                .onItem().transformToUni(state -> {
                    if (state.isFailing() || state.getMinResponseTime() > 5) {
                        LOG.warn("Payment processor is failing, using fallback strategy for correlationId: " + paymentRequest.getCorrelationId());
                        paymentRequest.setPaymentStrategy(PaymentStrategy.FALLBACK);
                        return fallbackClient.processPayment(paymentRequest)
                                .onItem().transformToUni(response -> {
                                    LOG.info("Payment processed successfully with fallback for correlationId: " + paymentRequest.getCorrelationId());
                                    String updatedPaymentString = JsonObject.mapFrom(paymentRequest).toString();
                                    return reactiveRedisClient.execute(Command.LPUSH, RedisJobState.COMPLETED_JOBS_KEY.getState(), updatedPaymentString);
                                });
                    } else {
                        LOG.info("Payment processor is healthy, processing payment for correlationId: " + paymentRequest.getCorrelationId());
                        paymentRequest.setPaymentStrategy(PaymentStrategy.DEFAULT);
                        return paymentProcessorDefaultClient.processPayment(paymentRequest)
                                .onItem().transformToUni(response -> {
                                    LOG.info("Payment processed successfully with default processor for correlationId: " + paymentRequest.getCorrelationId());
                                    String updatedPaymentString = JsonObject.mapFrom(paymentRequest).toString();
                                    return reactiveRedisClient.execute(Command.LPUSH, RedisJobState.COMPLETED_JOBS_KEY.getState(), updatedPaymentString);
                                });
                    }
                })
                .onFailure().recoverWithUni(failure -> {
                    LOG.error("Health check failed, using fallback strategy for correlationId: " + paymentRequest.getCorrelationId(), failure);
                    return fallbackClient.processPayment(paymentRequest)
                            .onItem().transformToUni(response -> {
                                LOG.info("Payment processed successfully with fallback after health check failure for correlationId: " + paymentRequest.getCorrelationId());
                                String failedPayment = JsonObject.mapFrom(paymentRequest).toString();
                                return reactiveRedisClient.execute(Command.LPUSH, RedisJobState.PAYMENT_REQUESTED.getState(), failedPayment);
                            });
                })
                .replaceWithVoid();
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
                                    return reactiveRedisClient.execute(Command.SETEX, RedisJobState.DEFAULT_PAYMENT_PROCESSOR_STATUS.getState(), "10", stateString)
                                            .onItem().transformToUni(x -> {
                                                LOG.info("Service health data persisted in Redis: " + stateString);
                                                return Uni.createFrom().item(state);
                                            });

                                }).onFailure().recoverWithItem(new PaymentProcessorState(false, 1));
                    }

                });
        }
}

