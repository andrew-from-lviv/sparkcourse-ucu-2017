package football_parser.utils.validation_utils;

import org.apache.spark.sql.DataFrame;

public interface DataFrameValidator {
    DataFrame validateDF(DataFrame data);
}
