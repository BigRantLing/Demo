import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.Charset;

public class UTFTests {
    public static void main(String[] args) {
        char ch = '\u0001';
        System.out.print("Hello");
        System.out.print(ch);
        System.out.print("world");

    }


    public static class Person{
        private String name;

        public Person(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
