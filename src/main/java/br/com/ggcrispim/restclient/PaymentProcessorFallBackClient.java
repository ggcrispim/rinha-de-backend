package br.com.ggcrispim.restclient;

import br.com.ggcrispim.dto.PaymentRequest;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;


@RegisterRestClient(configKey = "payment-processor-fallback")
public interface PaymentProcessorFallBackClient {

    @POST
    @Path("/payments")
    Uni<Response> processPayment(PaymentRequest paymentRequest);

}
