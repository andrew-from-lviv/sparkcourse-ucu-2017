package football_parser.services;

import football_parser.annotations.ShowResultsOnConsole;
import football_parser.utils.enrichment_utils.DataFrameEnricher;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;

@Service
public class DataEnrichmentService {
    private static final String VALIDATION_COLUMN_PATTERN = "IsValid";

    @Autowired
    private Collection<DataFrameEnricher> enrichers;

    @ShowResultsOnConsole
    public DataFrame enrich(DataFrame frame){
        DataFrame df = frame.toDF();
        for(DataFrameEnricher enricher: this.enrichers){
            df = enricher.enrich(df);
        }

        return df;
    }
}
