package football_parser.services;

import football_parser.configurations.UserConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class DataFrameBuilder {
    private static final String COLUMN_DELIMITER = ";";
    private static final String KEY_VALUE_DELIMITER = "=";

    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private SQLContext sqlContext;

    @Autowired
    private UserConfig userConfig;


    public DataFrame load() {
        String[] userDefinedColumns = userConfig.getColumnNames();
        JavaRDD<String> rdd = sc.textFile("data/football_parser/rawData.txt");
        JavaRDD<Row> rowJavaRDD = rdd
                .filter(line -> line != null && !line.isEmpty())
                .map(line -> parseRow(line, userDefinedColumns));

        return sqlContext.createDataFrame(rowJavaRDD, createStructType(userDefinedColumns));
    }

    private static Row parseRow(String row, String[] userDefinedColumns) {
        String[] data = row.split(COLUMN_DELIMITER);
        Map<String, String> keyValueMap = new HashMap<>();
        for(String column: data){
            String[] keyValue = column.split(KEY_VALUE_DELIMITER);
            keyValueMap.put(keyValue[0], keyValue[1]);
        }

        List<String> columnsData = new ArrayList<String>();

        for(String field: userDefinedColumns){
            if(keyValueMap.containsKey(field)){
                columnsData.add(keyValueMap.get(field));
            }
            else {
                columnsData.add("");
            }
        }

        return RowFactory.create(columnsData.toArray());
    }

    private static StructType createStructType(String[] columns){
        List<StructField> structFields = new ArrayList<>();
        for (String column: columns) {
            structFields.add(DataTypes.createStructField(column, DataTypes.StringType, true));
        }

        return DataTypes.createStructType(structFields);
    }
}