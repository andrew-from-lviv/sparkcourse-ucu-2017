package football_parser.utils;

import com.sun.org.apache.xpath.internal.operations.Bool;
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
    List<UDF1> udf1Functions;

    @Autowired
    List<UDF3> udf3Functions;


    @PostConstruct
    private void init(){
        for(UDF1 udf: this.udf1Functions){
            sqlContext.udf().register(udf.getClass().getName(), udf, DataTypes.BooleanType);
        }

        for(UDF3 udf: this.udf3Functions){
           sqlContext.udf().register(udf.getClass().getName(), udf, DataTypes.BooleanType);
        }
    }

}
