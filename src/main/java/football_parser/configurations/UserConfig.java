package football_parser.configurations;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Component
@PropertySource(value = "classpath:football_columns.properties")
public class UserConfig implements Serializable {
    @Value("${columnNames}")
    @Getter
    public String[] columnNames;

    @Getter
    public String[] players;

    @Getter
    public ArrayList<ArrayList<String>> teams;

    @SneakyThrows
    private Map<String, String> getPropValue(String propFile) {
        Properties properties  = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propFile));

        HashMap<String, String> propValueMap = new HashMap<>();
        val props = properties.propertyNames();

        while(props.hasMoreElements()) {
            String key = (String) props.nextElement();
            propValueMap.put(key, properties.getProperty(key));
        }

        return propValueMap;
    }
}
