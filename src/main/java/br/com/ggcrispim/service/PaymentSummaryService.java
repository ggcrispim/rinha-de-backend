package br.com.ggcrispim.service;

import br.com.ggcrispim.dto.PaymentSummary;
import br.com.ggcrispim.dto.Summary;
import br.com.ggcrispim.model.PaymentStrategy;
import br.com.ggcrispim.model.PaymentSummaryModel;
import br.com.ggcrispim.model.PaymentSummaryRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class PaymentSummaryService {

    private final PaymentSummaryRepository paymentSummaryRepository;

    @Inject
    public PaymentSummaryService(PaymentSummaryRepository paymentSummaryRepository) {
        this.paymentSummaryRepository = paymentSummaryRepository;
    }

    public PaymentSummary getPaymentSummary(String startDate, String endDate) {
        // Fetch the payment summaries from the repository based on the date range
        var paymentSummaries = paymentSummaryRepository.findByDateRange(parseDate(startDate), parseDate(endDate));

        // Split the payment summaries into two lists based on the payment strategy
        Map<PaymentStrategy, List<PaymentSummaryModel>> paymentsByStrategy = paymentSummaries.parallelStream()
                .collect(Collectors.groupingBy(PaymentSummaryModel::getPaymentStrategy));

        // Compute DEFAULT payments
        double totalDefault = paymentsByStrategy.getOrDefault(PaymentStrategy.DEFAULT, List.of()).parallelStream()
                .mapToDouble(PaymentSummaryModel::getAmount)
                .sum();
        Summary defaultSummary = new Summary(paymentsByStrategy.getOrDefault(PaymentStrategy.DEFAULT, List.of()).size(), totalDefault);

        // Compute FALLBACK payments
        double totalFallback = paymentsByStrategy.getOrDefault(PaymentStrategy.FALLBACK, List.of()).parallelStream()
                .mapToDouble(PaymentSummaryModel::getAmount)
                .sum();
        Summary fallbackSummary = new Summary(paymentsByStrategy.getOrDefault(PaymentStrategy.FALLBACK, List.of()).size(), totalFallback);


        // Convert the list of PaymentSummaryModel to PaymentSummary DTO
        return new PaymentSummary(defaultSummary, fallbackSummary);
    }

    private LocalDateTime parseDate(String date) {
        if(date != null && !date.isEmpty()) {
            Instant instant = Instant.parse(date);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        return null;
    }
}
