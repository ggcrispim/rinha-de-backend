package br.com.ggcrispim.model;



import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.LocalDateTime;
import java.util.List;

@ApplicationScoped
public class PaymentSummaryRepository implements PanacheRepository<PaymentSummaryModel> {

    public Uni<List<PaymentSummaryModel>> findByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        if (startDate == null && endDate == null) {
            return listAll();
        }

        if (startDate == null) {
            return find("requestedAt <= ?1", endDate).list();
        }

        if (endDate == null) {
            return find("requestedAt >= ?1", startDate).list();
        }

        return find("requestedAt >= ?1 and requestedAt <= ?2", startDate, endDate).list();
    }
}
