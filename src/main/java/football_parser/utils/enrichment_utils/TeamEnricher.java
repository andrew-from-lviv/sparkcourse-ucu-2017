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

import static football_parser.utils.Constants.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.Collection;
import java.util.Map;

@Service
@Configurable
public class TeamEnricher implements UDF1<String, String>, DataFrameEnricher  {

    @Autowired
    UserConfig userConfig;

    @Override
    public DataFrame enrich(DataFrame df) {
        return df.withColumn(FROM_TEAM_COLUMN_NAME, callUDF(this.getClass().getName(), col(FROM_COLUMN)))
                .withColumn(TO_TEAM_COLUMN_NAME, callUDF(this.getClass().getName(), col(TO_COLUMN)));
    }

    @Override
    @SneakyThrows
    public String call(String s){
        String team = TEAM_NOT_FOUND;
        for(Map.Entry<String, Collection<String>> entry: userConfig.teams.entrySet()){
            if(entry.getValue().contains(s.trim())){
                team = entry.getKey();
                break;
            }
        }

        return team;
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }
}
