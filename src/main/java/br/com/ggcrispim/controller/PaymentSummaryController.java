package br.com.ggcrispim.controller;

import br.com.ggcrispim.service.PaymentSummaryService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;


@Path("/payments-summary")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PaymentSummaryController {

    private final PaymentSummaryService paymentSummaryService;

    @Inject
    public PaymentSummaryController(PaymentSummaryService paymentSummaryService) {
        this.paymentSummaryService = paymentSummaryService;
    }

    @GET
    public Uni<Response> getPaymentSummary(@QueryParam("from") String from,
                                          @QueryParam("to") String to) {
        return paymentSummaryService.getPaymentSummary(from, to)
                .onItem().transform(summary -> Response.ok(summary).build())
                .onFailure().recoverWithItem(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Error retrieving payment summary").build());
    }
}
