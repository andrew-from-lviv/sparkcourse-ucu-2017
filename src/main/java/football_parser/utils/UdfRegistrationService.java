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
import java.util.Set;

@Service
public class UdfRegistrationService {

    private Collection<UDF1<String, Boolean>> UDF1Functions = new ArrayList<>();
    private Collection<UDF3<String, String, String, Boolean>> UDF3Functions = new ArrayList<>();

    @Autowired
    private SQLContext sqlContext;

    @SneakyThrows
    public UdfRegistrationService(){
        Reflections scanner = new Reflections("football_parser");

        Set<Class<? extends UDF1>> udf1s = scanner.getSubTypesOf(UDF1.class);
        for (Class<? extends UDF1> aClass : udf1s) {
            if (!Modifier.isAbstract(aClass.getModifiers())) {
                UDF1Functions.add(aClass.newInstance());
            }
        }

        Set<Class<? extends UDF3>> udf3s = scanner.getSubTypesOf(UDF3.class);
        for (Class<? extends UDF3> aClass : udf3s) {
            if (!Modifier.isAbstract(aClass.getModifiers())) {
                UDF3Functions.add(aClass.newInstance());
            }
        }
    }

    @PostConstruct
    private void init(){
        for(UDF1 udf: this.UDF1Functions){
            sqlContext.udf().register(udf.getClass().getName(), udf, DataTypes.BooleanType);
        }

        for(UDF3 udf: this.UDF3Functions){
           sqlContext.udf().register(udf.getClass().getName(), udf, DataTypes.BooleanType);
        }
    }

}
