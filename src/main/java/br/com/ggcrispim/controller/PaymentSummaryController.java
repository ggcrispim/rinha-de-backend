package br.com.ggcrispim.controller;


import br.com.ggcrispim.service.PaymentSummaryService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.LocalDateTime;


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
    public Response getPaymentSummary(@QueryParam("from") String from,
                                      @QueryParam("to") String to) {
        return Response.ok(paymentSummaryService.getPaymentSummary(from, to)).build();
    }
}
