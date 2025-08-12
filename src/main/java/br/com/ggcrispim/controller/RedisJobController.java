package br.com.ggcrispim.controller;

import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.service.RedisQueueService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/payments")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class RedisJobController {

    private final RedisQueueService queueService;

    @Inject
    public RedisJobController(RedisQueueService redisQueueService) {
        this.queueService = redisQueueService;
    }

    @POST
    public Uni<Void> enqueueJob(PaymentRequest paymentRequest) {
        return queueService.enqueueJob(paymentRequest)
                .onItem().ignore().andContinueWithNull()
                .onFailure().invoke(Unchecked.consumer(failure -> {
                    throw new RuntimeException("Failed to enqueue job: " + paymentRequest.getCorrelationId(), failure);
                }));
    }
}
