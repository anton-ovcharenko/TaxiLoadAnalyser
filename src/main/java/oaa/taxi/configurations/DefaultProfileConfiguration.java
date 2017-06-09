package oaa.taxi.configurations;

import oaa.taxi.services.LoadAnalyserService;
import oaa.taxi.services.impl.CsvSourceLoadAnalyserServiceImpl;
import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author aovcharenko date 09-06-2017.
 */
@Configuration
@Profile("default")
public class DefaultProfileConfiguration {
    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
            .setAppName("NY taxi load analyser (localhost)")
            .setMaster("local[*]")
            .set("spark.driver.host", "localhost")
            .set("spark.executor.memory", "8g")
            .set("spark.local.dir", "d:/temp/")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            ;
    }

    @Bean
    public LoadAnalyserService loadAnalyserService() {
        return new CsvSourceLoadAnalyserServiceImpl();
    }
}
