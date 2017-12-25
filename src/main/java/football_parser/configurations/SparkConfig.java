package football_parser.configurations;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Bean
    public SparkConf getSparkConf(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("football_parser")
                .setMaster("local[*]");

        return sparkConf;
    }
}
