package football_parser.services;

import football_parser.annotations.ShowResultsOnConsole;
import football_parser.utils.validation_utils.DataFrameValidator;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.spark.sql.functions.col;

@Service
public class DataValidationService {
    private static final String VALIDATION_COLUMN_PATTERN = "IsValid";

    @Autowired
    private Collection<DataFrameValidator> validators;

    @ShowResultsOnConsole
    public DataFrame validate(DataFrame frame){
        DataFrame df = frame.toDF();
        for(DataFrameValidator validator: this.validators){
            df = validator.validateDF(df);
        }

        return df;
    }

    @ShowResultsOnConsole
    public DataFrame removeInvalidRows(DataFrame dataFrame){
        Collection<String> columns = Arrays.asList(dataFrame.columns());
        DataFrame df = dataFrame.toDF();
        
        for(String column: columns){
            if(column.contains(VALIDATION_COLUMN_PATTERN)){
                df = df.filter(String.format("%s = true", column)).drop(col(column));
            }
        }

        return df;
    }
}
