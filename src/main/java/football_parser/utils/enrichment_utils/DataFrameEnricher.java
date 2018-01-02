package football_parser.utils.enrichment_utils;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataType;

public interface DataFrameEnricher {
    DataFrame enrich(DataFrame df);
    DataType getDataType();
}
