package transactions;

import football_parser.utils.validation_utils.EventTimeValidator;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author Evgeny Borisov
 * @since 3.2
 */
public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Conf.class);

        BusinessLogic businessLogic = context.getBean(BusinessLogic.class);
        businessLogic.doWork();
    }
}
