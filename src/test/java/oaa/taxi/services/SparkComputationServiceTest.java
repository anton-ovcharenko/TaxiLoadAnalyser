package oaa.taxi.services;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oaa.taxi.domain.Action;
import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.filters.ComputeXIndexFilter;
import oaa.taxi.domain.filters.ComputeYIndexFilter;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static oaa.taxi.services.LoadAnalyserService.generateSchema;

/**
 * @author aovcharenko date 01-06-2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class SparkComputationServiceTest implements Serializable {
    private static final int LEFT = 0;
    private static final int RIGHT = 4;
    private static final int BOTTOM = 0;
    private static final int TOP = 4;
    private static final int WIDTH = 4;
    private static final int HEIGHT = 4;
    private static final int ONE_HOUR_WINDOW = 60 * 60;
    private static final int ONE_DAY_WINDOW = ONE_HOUR_WINDOW * 24;
    private static final int FOUR_DAY_WINDOW = ONE_DAY_WINDOW * 4;

    private static SparkSession SS;
    private static SparkConf SC;
    private static JavaSparkContext JSC;

    private Dataset<Row> rowDataset;
    private SparkComputationService sparkComputationService;

    @BeforeClass
    public static void setUpClass() throws Exception {
        SC = new SparkConf();
        SC.setAppName("Test");
        SC.setMaster("local[*]");

        JSC = new JavaSparkContext(SC);
        SS = SparkSession.builder().sparkContext(JSC.sc()).config(SC).getOrCreate();
    }

    @Before
    public void setUp() throws Exception {
        ParametersHolder parametersHolder = new ParametersHolder();
        parametersHolder.setLeft(LEFT);
        parametersHolder.setRight(RIGHT);
        parametersHolder.setBottom(BOTTOM);
        parametersHolder.setTop(TOP);
        parametersHolder.setGridWidth(WIDTH);
        parametersHolder.setGridHeight(HEIGHT);
        parametersHolder.init();
        final Broadcast<ParametersHolder> broadcast = JSC.broadcast(parametersHolder);

        sparkComputationService = new SparkComputationService(broadcast);
        SS.udf().register(ComputeXIndexFilter.NAME, new ComputeXIndexFilter(broadcast), DataTypes.IntegerType);
        SS.udf().register(ComputeYIndexFilter.NAME, new ComputeYIndexFilter(broadcast), DataTypes.IntegerType);

        rowDataset = SS.read().format("CSV").option("header", "true").schema(generateSchema())
                       .load("./src/test/resources/test.csv");
    }

    @Test
    public void getLoadFactorListDropOff4D() throws Exception {
        List<LoadFactor> result =
            sparkComputationService.getLoadFactorList(rowDataset, Action.DROPOFF, getSeconds("2013-01-02 00:00:00"), FOUR_DAY_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 3, result.size());
        Assert.assertEquals(new LoadFactor(0, 1, 9), result.get(0));
        Assert.assertEquals(new LoadFactor(3, 0, 5), result.get(1));
        Assert.assertEquals(new LoadFactor(2, 2, 1), result.get(2));
    }

    @Test
    public void getLoadFactorListPickUp4D() throws Exception {
        List<LoadFactor> result =
            sparkComputationService.getLoadFactorList(rowDataset, Action.PICKUP, getSeconds("2013-01-02 00:00:00"), FOUR_DAY_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 5, result.size());
        Assert.assertEquals(new LoadFactor(1, 3, 6), result.get(0));
        Assert.assertEquals(new LoadFactor(3, 3, 4), result.get(1));
        Assert.assertEquals(new LoadFactor(1, 2, 3), result.get(2));
        Assert.assertEquals(new LoadFactor(0, 0, 2), result.get(3));
        Assert.assertEquals(new LoadFactor(1, 1, 1), result.get(4));
    }

    @Test
    public void getLoadFactorListDropOff1D() throws Exception {
        List<LoadFactor> result =
            sparkComputationService.getLoadFactorList(rowDataset, Action.DROPOFF, getSeconds("2013-01-03 00:00:00"), ONE_DAY_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 1, result.size());
        Assert.assertEquals(new LoadFactor(3, 0, 3), result.get(0));
    }

    @Test
    public void getLoadFactorListPickUp1D() throws Exception {
        List<LoadFactor> result =
            sparkComputationService.getLoadFactorList(rowDataset, Action.PICKUP, getSeconds("2013-01-04 00:00:00"), ONE_DAY_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 2, result.size());
        Assert.assertEquals(new LoadFactor(1, 3, 6), result.get(0));
        Assert.assertEquals(new LoadFactor(3, 3, 4), result.get(1));
    }

    @Test
    public void getLoadFactorListDropOff1H() throws Exception {
        List<LoadFactor> result =
            sparkComputationService.getLoadFactorList(rowDataset, Action.DROPOFF, getSeconds("2013-02-03 15:20:00"), ONE_HOUR_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 1, result.size());
        Assert.assertEquals(new LoadFactor(2, 3, 6), result.get(0));
    }

    @Test
    public void getLoadFactorListPickUp1H() throws Exception {
        List<LoadFactor> result =
            sparkComputationService.getLoadFactorList(rowDataset, Action.PICKUP, getSeconds("2013-01-04 02:00:00"), ONE_HOUR_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 0, result.size());
    }

    private long getSeconds(String stringTimestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = sdf.parse(stringTimestamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return TimeUnit.MILLISECONDS.toSeconds(date.getTime());
    }
}