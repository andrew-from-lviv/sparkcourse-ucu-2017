package football_parser.utils.validation_utils;

import football_parser.configurations.UserConfig;
import lombok.SneakyThrows;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Service;


import static football_parser.utils.Constants.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

@Service
@Configurable
public class EventTimeValidator implements DataFrameValidator, UDF1<String, Boolean> {



    @Autowired
    private UserConfig userConfig;

    @Override
    public DataFrame validateDF(DataFrame data) {
        return data.withColumn(EVENT_TIME_VALID_COLUMN, callUDF(this.getClass().getName(), col(EVENT_TIME_COLUMN)));
    }

    @Override
    @SneakyThrows
    public Boolean call(String s){
        String[] time = s.trim().split(TIME_DELIMITER);
        int minutes = Integer.parseInt(time[0]);
        int seconds = Integer.parseInt(time[1]);

        return (minutes > MAX_GAME_TIME || minutes < 0 || seconds < 0 || seconds > SECONDS_IN_MINUTE) ? false : true;
    }

    @Override
    public DataType getDataType() {
        return DataTypes.BooleanType;
    }
}
