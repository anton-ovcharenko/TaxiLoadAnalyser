package oaa.taxi;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;

@SpringBootApplication
public class SpringBootSparkApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootSparkApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
    }
}
