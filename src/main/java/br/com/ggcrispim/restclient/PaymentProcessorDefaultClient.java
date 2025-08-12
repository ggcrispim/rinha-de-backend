package br.com.ggcrispim.restclient;

import br.com.ggcrispim.dto.PaymentProcessorState;
import br.com.ggcrispim.dto.PaymentRequest;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;


@RegisterRestClient(configKey = "payment-processor-default")
public interface PaymentProcessorDefaultClient {

    @POST
    @Path("/payments")
    Uni<Response> processPayment(PaymentRequest paymentRequest);

    @GET
    @Path("/payments/service-health")
    Uni<PaymentProcessorState> serviceHealth();

}
