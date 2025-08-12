package br.com.ggcrispim.mapper;

import br.com.ggcrispim.dto.PaymentRequest;
import br.com.ggcrispim.model.PaymentSummaryModel;
import org.mapstruct.Mapper;


@Mapper(componentModel = "CDI")
public interface PaymentSummaryModelMapper {

    PaymentSummaryModel map(PaymentRequest source);
}