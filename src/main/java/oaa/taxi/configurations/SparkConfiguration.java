package oaa.taxi.configurations;

import oaa.taxi.domain.ParametersHolder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author aovcharenko date 24-05-2017.
 */
@Configuration
public class SparkConfiguration {

    @Bean
    public SparkConf sparkConf() {
        SparkConf conf = new SparkConf();
        conf.setAppName("NY taxi load analyser");
        conf.setMaster("local[1]");
        conf.set("spark.executor.memory", "8g");
        conf.set("spark.local.dir", "d:/temp/");
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
