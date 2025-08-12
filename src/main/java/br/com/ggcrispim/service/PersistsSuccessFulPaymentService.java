package br.com.ggcrispim.service;

import br.com.ggcrispim.config.RedisJobState;
import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.mapper.PaymentSummaryModelMapper;
import br.com.ggcrispim.model.PaymentSummaryRepository;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.scheduler.Scheduled;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PersistsSuccessFulPaymentService {

    private final RedisDataSource redisClient;
    private final PaymentSummaryRepository paymentSummaryRepository;
    private final PaymentSummaryModelMapper paymentSummaryModelMapper;

    @Inject
    public PersistsSuccessFulPaymentService (RedisDataSource redisClient,
                                             PaymentSummaryRepository paymentSummaryRepository,
                                             PaymentSummaryModelMapper paymentSummaryModelMapper) {
        this.redisClient = redisClient;
        this.paymentSummaryRepository = paymentSummaryRepository;
        this.paymentSummaryModelMapper = paymentSummaryModelMapper;
    }

    private static final Logger LOG = Logger.getLogger(RedisQueueService.class);

    @Scheduled(every = "1s")
    @Transactional
    void dequeueAndPersistSuccessfulPayments() {
        if(redisClient.key().exists(RedisJobState.COMPLETED_JOBS_KEY.getState())) {
            String paymentString =  redisClient.execute(Command.RPOP, RedisJobState.COMPLETED_JOBS_KEY.getState()).toString();
            PaymentRequest paymentRequest = new JsonObject(paymentString).mapTo(PaymentRequest.class);
            LOG.info("Job dequeued with correlationId: " + paymentRequest.getCorrelationId());
            paymentSummaryRepository.persist(paymentSummaryModelMapper.map(paymentRequest));
            LOG.info("Payment persisted successfully: " + paymentRequest.getCorrelationId());
        }
    }
}
