package oaa.taxi.services;

import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static oaa.taxi.services.LoadAnalyserService.generateSchema;

/**
 * Created by Antman on 05.06.2017.
 */

@RunWith(SpringJUnit4ClassRunner.class)
abstract public class BaseSparkComputationServiceTest {
    protected static final int LEFT = 0;
    protected static final int RIGHT = 4;
    protected static final int BOTTOM = 0;
    protected static final int TOP = 4;
    protected static final int WIDTH = 4;
    protected static final int HEIGHT = 4;
    protected static final int ONE_HOUR_WINDOW = 60 * 60;
    protected static final int ONE_DAY_WINDOW = ONE_HOUR_WINDOW * 24;
    protected static final int FOUR_DAY_WINDOW = ONE_DAY_WINDOW * 4;


    protected static SparkSession SS;
    protected static SparkConf SC;
    protected static JavaSparkContext JSC;

    protected Dataset<Row> rowDataset;

    @BeforeClass
    public static void setUpClass() throws Exception {
        SC = new SparkConf();
        SC.setAppName("Test");
        SC.setMaster("local[1]");

        JSC = new JavaSparkContext(SC);
        SS = SparkSession.builder().sparkContext(JSC.sc()).config(SC).getOrCreate();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if(SS != null) {
            SS.stop();
        }

        if(JSC != null) {
            JSC.stop();
        }
    }

    protected void setUp() throws Exception {
        rowDataset = SS.read().format("CSV").option("header", "true").schema(generateSchema())
                .load("./src/test/resources/test.csv");
    }

    protected Broadcast<ParametersHolder> createBroadcastParameterHolder() {
        ParametersHolder parametersHolder = new ParametersHolder();
        parametersHolder.setLeft(LEFT);
        parametersHolder.setRight(RIGHT);
        parametersHolder.setBottom(BOTTOM);
        parametersHolder.setTop(TOP);
        parametersHolder.setGridWidth(WIDTH);
        parametersHolder.setGridHeight(HEIGHT);
        parametersHolder.init();
        return JSC.broadcast(parametersHolder);
    }

    protected long getSeconds(String stringTimestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = sdf.parse(stringTimestamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return TimeUnit.MILLISECONDS.toSeconds(date.getTime());
    }

    protected void checkLoadFactorListDropOff4D(List<LoadFactor> result) {
        Assert.assertEquals("Amount of LoadFactors is wrong", 3, result.size());
        Assert.assertEquals(new LoadFactor(0, 1, 9), result.get(0));
        Assert.assertEquals(new LoadFactor(3, 0, 5), result.get(1));
        Assert.assertEquals(new LoadFactor(2, 2, 1), result.get(2));
    }

    protected void checkLoadFactorListPickUp4D(List<LoadFactor> result) {
        Assert.assertEquals("Amount of LoadFactors is wrong", 5, result.size());
        Assert.assertEquals(new LoadFactor(1, 3, 6), result.get(0));
        Assert.assertEquals(new LoadFactor(3, 3, 4), result.get(1));
        Assert.assertEquals(new LoadFactor(1, 2, 3), result.get(2));
        Assert.assertEquals(new LoadFactor(0, 0, 2), result.get(3));
        Assert.assertEquals(new LoadFactor(1, 1, 1), result.get(4));
    }

    protected void checkLoadFactorListDropOff1D(List<LoadFactor> result) {
        Assert.assertEquals("Amount of LoadFactors is wrong", 1, result.size());
        Assert.assertEquals(new LoadFactor(3, 0, 3), result.get(0));
    }

    protected void checkLoadFactorListPickUp1D(List<LoadFactor> result) {
        Assert.assertEquals("Amount of LoadFactors is wrong", 2, result.size());
        Assert.assertEquals(new LoadFactor(1, 3, 6), result.get(0));
        Assert.assertEquals(new LoadFactor(3, 3, 4), result.get(1));
    }

    protected void checkLoadFactorListDropOff1H(List<LoadFactor> result) {
        Assert.assertEquals("Amount of LoadFactors is wrong", 1, result.size());
        Assert.assertEquals(new LoadFactor(2, 3, 6), result.get(0));
    }

    protected void checkLoadFactorListPickUp1H(List<LoadFactor> result) {
        Assert.assertEquals("Amount of LoadFactors is wrong", 0, result.size());
    }
}
