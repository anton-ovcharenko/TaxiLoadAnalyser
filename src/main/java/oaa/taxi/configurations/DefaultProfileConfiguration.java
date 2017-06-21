package oaa.taxi.configurations;

import oaa.taxi.services.LoadAnalyserService;
import oaa.taxi.services.impl.CsvSourceLoadAnalyserServiceImpl;
import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author aovcharenko date 09-06-2017.
 */
@Configuration
public class DefaultProfileConfiguration {
    @Value("${spark.local.dir}")
    private String sparkDir;

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
            .setAppName("NY taxi load analyser (localhost)")
            .setMaster("local[*]")
            .set("spark.driver.host", "localhost")
            .set("spark.executor.memory", "8g")
            .set("spark.local.dir", sparkDir)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            ;
    }

    @Bean
    public LoadAnalyserService loadAnalyserService() {
        return new CsvSourceLoadAnalyserServiceImpl();
    }
}
