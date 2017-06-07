package oaa.taxi.configurations;

import oaa.taxi.domain.ParametersHolder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan("oaa.taxi")
@PropertySource("classpath:property.properties")
public class ApplicationConfiguration {

    @Bean
    @Profile("localhost")
    public SparkConf sparkConfLocalhost() {
        return new SparkConf()
                .setAppName("NY taxi load analyser (localhost)")
                .setMaster("local[4]")
                .set("spark.executor.memory", "2g")
                .set("spark.local.dir", "d:/temp/")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                ;
    }

    @Bean
    @Profile("default")
    public SparkConf sparkConfProd() {
        //not ready now
        return new SparkConf()
                .setAppName("NY taxi load analyser (PROD)")
                .setMaster("mesos://HOST:PORT")
                ;
    }

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
