package br.com.ggcrispim.service;


import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.model.PaymentSummaryRepository;
import br.com.ggcrispim.producer.PaymentStreamProducer;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;

@ApplicationScoped
public class RedisQueueService {

    private final PaymentStreamProducer paymentStreamProducer;
    private final PaymentService paymentService;

    @Inject
    public RedisQueueService(ReactiveRedisDataSource reactiveRedisClient, PaymentSummaryRepository paymentSummaryRepository, PaymentStreamProducer paymentStreamProducer,
            PaymentService paymentService) {
        this.paymentStreamProducer = paymentStreamProducer;
        this.paymentService = paymentService;
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

    public Uni<PaymentRequest> processPaymentWithHealthCheck(PaymentRequest paymentRequest) {
        return paymentService.processPayment(paymentRequest)
                .onItem().transform(response -> {
                    LOG.info("Payment processed successfully for correlationId: " + paymentRequest.getCorrelationId());
                    return paymentRequest;
                });
    }

}

