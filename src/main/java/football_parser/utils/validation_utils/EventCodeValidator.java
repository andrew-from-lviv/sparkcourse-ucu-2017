package football_parser.utils.validation_utils;

import football_parser.configurations.UserConfig;
import lombok.SneakyThrows;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;

import static football_parser.utils.Constants.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 * Validates number of players for the event
 * User existence is NOT validated in this class - The separate validator should be used
 */

@Service
@Configurable
public class EventCodeValidator implements DataFrameValidator, UDF3<String, String, String, Boolean> {
    @Autowired
    UserConfig userConfig;

    Collection<String> existingPlayers;
    Collection<String> singlePlayerEvents;
    //String[] doublePlayersEvents;

    @PostConstruct
    private void init(){
        this.existingPlayers = userConfig.getAllPlayers();
        this.singlePlayerEvents = Arrays.asList(userConfig.getSinglePlayerEvents());
    }

    @Override
    public DataFrame validateDF(DataFrame data) {
        return data.withColumn(EVENT_CODE_VALID_COLUMN, callUDF(this.getClass().getName(), col(EVENT_CODE_COLUMN), col(FROM_COLUMN), col(TO_COLUMN)));
    }

    @Override
    public DataType getDataType() {
        return DataTypes.BooleanType;
    }

    @Override
    @SneakyThrows
    public Boolean call(String code, String from, String to) {
        if(this.singlePlayerEvents.contains(code)){
            return (to.isEmpty() || to == null) && (from != null || !from.isEmpty());
        }
        return !to.isEmpty() && !from.isEmpty() && from != null && to != null;
    }
}
