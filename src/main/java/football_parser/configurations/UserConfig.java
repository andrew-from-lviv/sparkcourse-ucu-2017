package football_parser.configurations;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.*;

import static football_parser.utils.Constants.*;

@Component
public class UserConfig implements Serializable {
    @Value("${columnNames}")
    @Getter
    public String[] columnNames;

    @Value("${singlePlayerEvents}")
    @Getter
    public String[] singlePlayerEvents;

    @Value("$doublePlayersEvents")
    @Getter
    public String[] doublePlayersEvents;

    @Getter
    public Collection<String> allPlayers;

    @Getter
    public Map<String, Collection<String>> teams;
    
    @Getter
    public Map<String, String> codes;

    @PostConstruct
    private void init(){
        this.teams = getTeams(TEAM_PROP_FILE);
        this.codes = getPropValue(CODES_PROP_FILE);
        this.allPlayers = initAllPlayers(this.teams);
    }

    private Collection<String> initAllPlayers(Map<String, Collection<String>> teams){
        Collection<String> teamPlayers = new ArrayList<String>();
        for(val team: this.teams.entrySet()){
            teamPlayers.addAll(team.getValue());
        }
        return teamPlayers;
    }


    private Map<String, Collection<String>> getTeams(String propsFile){
        val teamsData = getPropValue(propsFile);
        Map<String, Collection<String>> teams = new HashMap<>();
        for(val teamName: teamsData.keySet()){
            Collection<String> theTeam = Arrays.asList(teamsData.get(teamName).trim().split(PLAYERS_DELIMITER));
            teams.put(teamName, theTeam);
        }

        return teams;
    }

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
