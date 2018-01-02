package football_parser.aspects;

import org.apache.spark.sql.DataFrame;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Profile;

import org.springframework.stereotype.Service;

import static football_parser.configurations.Profiles.DEV;

@Aspect
@Service
@Profile(DEV)
@Configurable
@EnableAspectJAutoProxy
public aspect ShowResultsOnConsoleAspect {
    @AfterReturning(value = "@annotation(ShowResultsOnConsole)", returning = "df")
    public void showResultsOnConsole(DataFrame df){

        df.show();
        System.out.println("Was printed by My nice Aspect only in DEV...");
    }
}
