package football_parser;

import football_parser.configurations.UserConfig;
import football_parser.services.DataFrameBuilder;
import football_parser.services.DataValidationService;
import football_parser.utils.UdfRegistrationService;
import org.apache.spark.sql.DataFrame;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);

        DataFrameBuilder dfBuilder = context.getBean(DataFrameBuilder.class);
        DataFrame df = dfBuilder.load();
        df.show();

        context.getBean(UdfRegistrationService.class);

        DataValidationService validator = context.getBean(DataValidationService.class);
        DataFrame validatedDf = validator.validate(df);
        validatedDf.show();
    }
}
