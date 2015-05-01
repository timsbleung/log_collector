import java.util.HashMap;
import java.util.Map;

/**
 * Created by tl on 4/26/15.
 */
public class log_packet {

    public String timestamp;
    public String event;
    public String eventid;
    public String user;
    public String source;
    public Map<String, String> metadata = new HashMap<>();



    public String get_string() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"timestamp\" :" +"\""+timestamp+"\",");
        sb.append("\"event\" :" +"\""+event+"\",");
        sb.append("\"eventid\" :" +"\""+eventid+"\",");
        sb.append("\"user\" :" +"\""+user+"\",");
        sb.append("\"source\" :" +"\""+source+"\",");
        sb.append("\"metadata\" :" + map_to_string(metadata));
        sb.append(" }");
        return sb.toString();
    }

    static String map_to_string(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<String, String> entry : map.entrySet())
            sb.append("\"" + entry.getKey() + "\": \"" + entry.getValue() + "\", ");
        sb.deleteCharAt(sb.length()-1); sb.deleteCharAt(sb.length()-1);
        sb.append("}");
        return sb.toString();
    }

}
