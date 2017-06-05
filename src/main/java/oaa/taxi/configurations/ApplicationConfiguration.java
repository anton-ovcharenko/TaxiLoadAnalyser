package oaa.taxi.configurations;

import oaa.taxi.domain.ParametersHolder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.*;

@Configuration
@ComponentScan("oaa.taxi")
@PropertySource("classpath:property.properties")
public class ApplicationConfiguration {


    @Bean
    @Profile("localhost")
    public SparkConf sparkConfLocalhost() {
        SparkConf conf = new SparkConf();
        conf.setAppName("NY taxi load analyser (localhost)");
        conf.setMaster("local[2]");
        conf.set("spark.executor.memory", "1g");
        conf.set("spark.local.dir", "d:/temp/");
        return conf;
    }

    @Bean
    @Profile("default")
    public SparkConf sparkConfProd() {
        SparkConf conf = new SparkConf();
        conf.setAppName("NY taxi load analyser (PROD)");
        conf.setMaster("local[*]");
        return conf;
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
