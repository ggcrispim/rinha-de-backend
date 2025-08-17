package br.com.ggcrispim.service;

import br.com.ggcrispim.dto.PaymentSummary;
import br.com.ggcrispim.dto.Summary;
import br.com.ggcrispim.model.PaymentSummaryModel;
import br.com.ggcrispim.model.PaymentSummaryRepository;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

@ApplicationScoped
public class PaymentSummaryService {

    private final PaymentSummaryRepository paymentSummaryRepository;
    private final Pool client;
    private static final Logger LOG = Logger.getLogger(RedisQueueService.class);

    @Inject
    public PaymentSummaryService(PaymentSummaryRepository paymentSummaryRepository, Pool client) {
        this.paymentSummaryRepository = paymentSummaryRepository;
        this.client = client;
    }

    public Uni<PaymentSummary> getPaymentSummary(String startDate, String endDate) {
        return PaymentSummaryModel.findPaymentsByDateRange(client, parseDate(startDate), parseDate(endDate))
                .onItem().transformToUni(payments -> {
                    if (payments.isEmpty()) {
                      return Uni.createFrom().item(new PaymentSummary(new Summary(0, 0), new Summary(0, 0)));
                    } else {
                        return calculatePaymentSummary(payments);
                    }

                })
                .onFailure().invoke( failure -> {
                            LOG.error("Error to retrieve data: " + failure.getMessage());
                });

    }

    private Uni<PaymentSummary> calculatePaymentSummary(List<PaymentSummaryModel> payments) {
        return Uni.createFrom().item(new PaymentSummary(
                new Summary(payments.size(), payments.size() * 19.90),
                new Summary(0, 0)));
    }

    private LocalDateTime parseDate(String date) {
        Instant instant = Instant.parse(date);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
