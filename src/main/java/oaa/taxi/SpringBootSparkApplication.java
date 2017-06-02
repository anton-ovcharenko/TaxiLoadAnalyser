package oaa.taxi;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootSparkApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootSparkApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
    }
}
