package oaa.taxi.configurations;

import oaa.taxi.domain.ParametersHolder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan("oaa.taxi")
@PropertySource("classpath:property.properties")
public class ApplicationConfiguration {
    @Bean
    public JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    public SparkSession sparkSession(JavaSparkContext javaSparkContext, SparkConf sparkConf) {
        return SparkSession.builder()
                           .sparkContext(javaSparkContext.sc())
                           .config(sparkConf)
                           .getOrCreate();
    }

    @Bean
    Broadcast<ParametersHolder> parametersHolderBroadcast(JavaSparkContext javaSparkContext, ParametersHolder parametersHolder) {
        return javaSparkContext.broadcast(parametersHolder);
    }
}
