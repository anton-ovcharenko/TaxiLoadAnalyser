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
@Profile("databricks")
public class DatabricksProfileConfiguration {
    //NOT WORK FOR NOW

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
            .setAppName("NY taxi load analyser (PROD)")

            .setMaster("spark://ec2-34-210-52-31.us-west-2.compute.amazonaws.com:8080")
            .set("spark.driver.host","ec2-34-210-52-31.us-west-2.compute.amazonaws.com")
            .set("spark.driver.port","8080")

            //.setMaster("spark://ec2-34-210-52-31.us-west-2.compute.amazonaws.com:10000")
            //.setMaster("spark://10.172.238.66:41936")
            //.setMaster("spark://ec2-34-210-52-31.us-west-2.compute.amazonaws.com:41936")
            //.setSparkHome("ec2-34-210-52-31.us-west-2.compute.amazonaws.com")
            ;
    }

    @Bean
    public LoadAnalyserService loadAnalyserService() {
        return new CsvSourceLoadAnalyserServiceImpl();
    }
}
