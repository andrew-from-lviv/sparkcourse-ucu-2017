package football_parser.services;

import football_parser.utils.validation_utils.DataFrameValidator;
import lombok.SneakyThrows;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;
import org.reflections.Reflections;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

@Service
public class DataValidationService {
    private Collection<DataFrameValidator> validators = new ArrayList<>();

    @SneakyThrows
    public DataValidationService(){
        Reflections scanner = new Reflections("football_parser");
        Set<Class<? extends DataFrameValidator>> classes = scanner.getSubTypesOf(DataFrameValidator.class);
            for (Class<? extends DataFrameValidator> aClass : classes) {
                if (!Modifier.isAbstract(aClass.getModifiers())) {
                    validators.add(aClass.newInstance());
                }
            }
    }

    public DataFrame validate(DataFrame frame){
        DataFrame newFrame = frame.toDF();
        for(DataFrameValidator validator: this.validators){
            newFrame = validator.validateDF(newFrame);
        }

        return newFrame;
    }
}
