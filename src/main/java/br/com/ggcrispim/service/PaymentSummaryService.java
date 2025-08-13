package br.com.ggcrispim.service;

import br.com.ggcrispim.dto.PaymentSummary;
import br.com.ggcrispim.dto.Summary;
import br.com.ggcrispim.model.PaymentStrategy;
import br.com.ggcrispim.model.PaymentSummaryModel;
import br.com.ggcrispim.model.PaymentSummaryRepository;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@ApplicationScoped
public class PaymentSummaryService {

    private final PaymentSummaryRepository paymentSummaryRepository;

    @Inject
    public PaymentSummaryService(PaymentSummaryRepository paymentSummaryRepository) {
        this.paymentSummaryRepository = paymentSummaryRepository;
    }

    @WithSession
    public Uni<PaymentSummary> getPaymentSummary(String startDate, String endDate) {
        return paymentSummaryRepository.findByDateRange(parseDate(startDate), parseDate(endDate))
                .onItem().transform(paymentSummaries -> {
                    // Use more efficient grouping
                    double totalDefault = 0;
                    double totalFallback = 0;
                    int countDefault = 0;
                    int countFallback = 0;

                    for (PaymentSummaryModel payment : paymentSummaries) {
                        if (payment.getPaymentStrategy() == PaymentStrategy.DEFAULT) {
                            totalDefault += payment.getAmount();
                            countDefault++;
                        } else if (payment.getPaymentStrategy() == PaymentStrategy.FALLBACK) {
                            totalFallback += payment.getAmount();
                            countFallback++;
                        }
                    }

                    Summary defaultSummary = new Summary(countDefault, totalDefault);
                    Summary fallbackSummary = new Summary(countFallback, totalFallback);

                    return new PaymentSummary(defaultSummary, fallbackSummary);
                });
    }

    private LocalDateTime parseDate(String date) {
        if(date != null && !date.isEmpty()) {
            Instant instant = Instant.parse(date);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        return null;
    }
}
