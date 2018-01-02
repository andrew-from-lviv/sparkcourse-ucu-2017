package football_parser.utils;

public class Constants {
    public static final String TIME_DELIMITER = ":";
    public static final int MAX_GAME_TIME = 100;
    public static final int SECONDS_IN_MINUTE = 60;
    public static final String VALIDATION_FALSE_VALUE = "0";
    public static final String VALIDATION_TRUE_VALUE = "1";
    public static final String TEAM_PROP_FILE = "teams.properties";
    public static final String PLAYERS_DELIMITER = ",";
    public static final String CODES_PROP_FILE = "codes.properties";
    public static final String FROM_VALID_COLUMN = "fromIsValid";
    public static final String FROM_COLUMN = "from";
    public static final String TO_COLUMN = "to";
    public static final String TO_VALID_COLUMN = "toIsValid";
    public static final String EVENT_CODE_VALID_COLUMN = "codeIsValid";
    public static final String EVENT_CODE_COLUMN = "code";
    public static final String TEAM_NOT_FOUND = "TeamNotFound";
    public static final String FROM_TEAM_COLUMN_NAME = "FromPlayerTeam";
    public static final String TO_TEAM_COLUMN_NAME = "ToPlayerTeam";
    public static final String EVENT_TIME_VALID_COLUMN = "eventTimeIsValid";
    public static final String EVENT_TIME_COLUMN = "eventTime";
    public static final int FIRST_TIME_START = 0;
    public static final int FIRST_TIME_END = 45;
    public static final int SECOND_TIME_START = 45;
    public static final int SECOND_TIME_END = MAX_GAME_TIME; //Additional time
    public static final String TIME_COLUMN = "Time";
    public static final int FIRST_TIME = 1;
    public static final int SECOND_TIME = 2;
    public static final int TIME_INVALID = -1;
    public static final String OPERATION_NOT_FOUND = "OperationNotFound";
    public static final String OPERATION_DESCRIPTION = "OperationDescription";

}
