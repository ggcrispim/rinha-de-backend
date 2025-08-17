package br.com.ggcrispim.model;

import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.LocalDateTime;
import java.util.List;

@ApplicationScoped
public class PaymentSummaryRepository implements PanacheRepository<PaymentSummaryModel> {
}
