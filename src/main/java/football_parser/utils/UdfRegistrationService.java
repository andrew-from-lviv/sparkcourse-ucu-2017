package football_parser.utils;

import com.sun.org.apache.xpath.internal.operations.Bool;
import football_parser.services.DataValidationService;
import football_parser.utils.enrichment_utils.DataFrameEnricher;
import football_parser.utils.validation_utils.DataFrameValidator;
import lombok.SneakyThrows;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.reflections.Reflections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.ws.rs.POST;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Service
public class UdfRegistrationService {
    @Autowired
    private SQLContext sqlContext;

    @Autowired
    List<DataFrameValidator> validators;

    @Autowired
    List<DataFrameEnricher> enrichers;


    @PostConstruct
    private void init(){
        for(DataFrameValidator validator: this.validators){
            if(validator instanceof UDF1){
                sqlContext.udf().register(validator.getClass().getName(), (UDF1)validator, validator.getDataType());
            }
            else {
                sqlContext.udf().register(validator.getClass().getName(), (UDF3)validator, validator.getDataType());
            }
        }

        for(DataFrameEnricher enricher: this.enrichers){
            if(enricher instanceof UDF1){
                sqlContext.udf().register(enricher.getClass().getName(), (UDF1)enricher, enricher.getDataType());
            }
            else {
                sqlContext.udf().register(enricher.getClass().getName(), (UDF3)enricher, enricher.getDataType());
            }
        }
    }

}
