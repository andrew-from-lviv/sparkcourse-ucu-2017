package football_parser.utils.validation_utils;

import football_parser.configurations.UserConfig;
import football_parser.utils.Constants;
import lombok.SneakyThrows;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.api.java.UDF1;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collection;

import static football_parser.utils.Constants.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

@Service
@Configurable
public class PlayerExistenceValidator implements DataFrameValidator, UDF1<String, Boolean> {
    @Autowired
    UserConfig userConfig;

    Collection<String> existingPlayers;

    @PostConstruct
    // Saving existingPlayers to avoid loading them each time
    private void init(){
        this.existingPlayers = userConfig.getAllPlayers();
    }

    @Override
    public DataFrame validateDF(DataFrame data) {
        return data.withColumn(FROM_VALID_COLUMN, callUDF(this.getClass().getName(), col(FROM_COLUMN)))
                .withColumn(TO_VALID_COLUMN, callUDF(this.getClass().getName(), col(TO_COLUMN)));
    }

    @Override
    @SneakyThrows
    public Boolean call(String s) {
        if(this.existingPlayers.contains(s.trim())){
            return true;
        }

        return false;
    }
}
