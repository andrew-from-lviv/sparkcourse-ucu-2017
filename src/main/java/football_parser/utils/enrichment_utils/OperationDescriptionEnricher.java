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
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

@Service
@Configurable
public class OperationDescriptionEnricher implements UDF1<String, String>, DataFrameEnricher  {

    @Autowired
    UserConfig userConfig;

    @Override
    public DataFrame enrich(DataFrame df) {
        return df.withColumn(OPERATION_DESCRIPTION, callUDF(this.getClass().getName(), col(EVENT_CODE_COLUMN)));
    }

    @Override
    @SneakyThrows
    public String call(String s){
        String desc = userConfig.codes.get(s.trim());
        return desc != null ? desc : OPERATION_NOT_FOUND;
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }
}
