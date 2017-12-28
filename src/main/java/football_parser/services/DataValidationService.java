package football_parser.services;

import football_parser.utils.validation_utils.DataFrameValidator;
import lombok.SneakyThrows;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.reflections.Reflections;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import static org.apache.spark.sql.functions.col;

@Service
public class DataValidationService {
    private static final String VALIDATION_COLUMN_PATTERN = "IsValid";

    @Autowired
    private Collection<DataFrameValidator> validators;

    public DataFrame validate(DataFrame frame){
        DataFrame newFrame = frame.toDF();
        for(DataFrameValidator validator: this.validators){
            newFrame = validator.validateDF(newFrame);
        }

        return newFrame;
    }

    public DataFrame removeInvalidRows(DataFrame df){
        Collection<String> columns = Arrays.asList(df.columns());
        DataFrame validated = null;
        
        for(String column: columns){
            if(column.contains(VALIDATION_COLUMN_PATTERN)){
                validated = df.filter(String.format("%s = true", column));
            }
        }

        return validated;
    }
}
