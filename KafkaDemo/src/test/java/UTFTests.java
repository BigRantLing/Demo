import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UTFTests {
    public static void main(String[] args) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Event event = new Event();
        event.setContent("content");
        event.setScore(2.3f);
        event.setId(1);
        List<String> mates = new ArrayList<>();
        mates.add("qiang");
        mates.add("ming");
        event.setMates(mates);
        event.setName("xiao hogn");
        event.setLogDate("2020-12-25");

        System.out.println(mapper.writeValueAsString(event));

    }
}
