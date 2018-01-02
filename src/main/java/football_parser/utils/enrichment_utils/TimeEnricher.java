package football_parser.utils.enrichment_utils;

import football_parser.configurations.UserConfig;
import lombok.SneakyThrows;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;

import static football_parser.utils.Constants.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
@Configurable
public class TimeEnricher implements UDF1<String, Integer>, DataFrameEnricher  {

    @Override
    public DataFrame enrich(DataFrame df) {
        return df.withColumn(TIME_COLUMN, callUDF(this.getClass().getName(), col(EVENT_TIME_COLUMN)));
    }

    @Override
    @SneakyThrows
    public Integer call(String s){
        String[] time = s.trim().split(TIME_DELIMITER);
        int minutes = Integer.parseInt(time[0]);
        return minutes > FIRST_TIME_START && minutes < FIRST_TIME_END ? FIRST_TIME : (minutes > SECOND_TIME_START && minutes < SECOND_TIME_END ? SECOND_TIME : TIME_INVALID);
    }

    @Override
    public DataType getDataType() {
        return DataTypes.IntegerType;
    }
}
