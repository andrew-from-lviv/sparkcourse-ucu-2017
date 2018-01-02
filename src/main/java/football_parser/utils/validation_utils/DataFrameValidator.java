package football_parser.utils.validation_utils;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataType;

public interface DataFrameValidator {
    DataFrame validateDF(DataFrame data);
    DataType getDataType();
}
