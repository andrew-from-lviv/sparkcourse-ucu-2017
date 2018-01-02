package football_parser;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;

@Configuration
@ComponentScan(basePackages = "football_parser.*")
@PropertySource(value = "classpath:football_columns.properties")
@PropertySource(value = "classpath:single_player_events.properties")
@PropertySource(value = "classpath:double_players_events.properties")
@EnableAspectJAutoProxy
public class Config {

    @Autowired
    private SparkConf sparkConf;

    @Bean
    public JavaSparkContext sc() {
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sc());
    }

}
