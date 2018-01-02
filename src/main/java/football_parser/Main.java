package football_parser;

import football_parser.configurations.UserConfig;
import football_parser.services.DataEnrichmentService;
import football_parser.services.DataFrameBuilder;
import football_parser.services.DataValidationService;
import football_parser.utils.UdfRegistrationService;
import football_parser.utils.enrichment_utils.DataFrameEnricher;
import org.apache.spark.sql.DataFrame;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.xml.crypto.Data;

public class Main {
    public static void main(String[] args) {
        System.setProperty("spring.profiles.active", "Dev");
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);

        DataFrameBuilder dfBuilder = context.getBean(DataFrameBuilder.class);
        DataFrame df = dfBuilder.load();
        df.show();

        context.getBean(UdfRegistrationService.class);

        DataValidationService validator = context.getBean(DataValidationService.class);
        DataFrame validatedDf = validator.validate(df);
        validatedDf.show();

        DataFrame onlyValidDf = validator.removeInvalidRows(validatedDf);
        onlyValidDf.show();

        DataEnrichmentService enricher = context.getBean(DataEnrichmentService.class);
        DataFrame enrichedValidatedDf = enricher.enrich(onlyValidDf);
        enrichedValidatedDf.show();

        DataFrame enrichedInitialDf = enricher.enrich(df);
        enrichedInitialDf.show();
    }
}
