package oaa.taxi.services;

import java.util.List;

import oaa.taxi.domain.Action;
import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.filters.ComputeXIndexFilter;
import oaa.taxi.domain.filters.ComputeYIndexFilter;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author aovcharenko date 01-06-2017.
 */
public class SparkComputationService3ImplTest extends BaseSparkComputationServiceTest {

    private SparkComputationService3Impl sparkComputationService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Broadcast<ParametersHolder> broadcastParameterHolder = createBroadcastParameterHolder();

        sparkComputationService = new SparkComputationService3Impl(broadcastParameterHolder);

        SS.udf().register(
                ComputeXIndexFilter.NAME,
                new ComputeXIndexFilter(broadcastParameterHolder),
                DataTypes.IntegerType);
        SS.udf().register(
                ComputeYIndexFilter.NAME,
                new ComputeYIndexFilter(broadcastParameterHolder),
                DataTypes.IntegerType);
    }

    @Test
    public void getLoadFactorListDropOff4D() throws Exception {
        List<LoadFactor> result = sparkComputationService.getLoadFactorList(
                rowDataset,
                Action.DROPOFF,
                getSeconds("2013-01-02 00:00:00"),
                FOUR_DAY_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 3, result.size());
        Assert.assertEquals(new LoadFactor(2, 2, 1), result.get(0));
        Assert.assertEquals(new LoadFactor(0, 1, 24), result.get(1));
        Assert.assertEquals(new LoadFactor(3, 0, 5), result.get(2));
    }

    @Test
    public void getLoadFactorListPickUp4D() throws Exception {
        List<LoadFactor> result = sparkComputationService.getLoadFactorList(
                rowDataset,
                Action.PICKUP,
                getSeconds("2013-01-02 00:00:00"),
                FOUR_DAY_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 5, result.size());
        Assert.assertEquals(new LoadFactor(1, 2, 3), result.get(0));
        Assert.assertEquals(new LoadFactor(1, 1, 10), result.get(1));
        Assert.assertEquals(new LoadFactor(1, 3, 6), result.get(2));
        Assert.assertEquals(new LoadFactor(0, 0, 2), result.get(3));
        Assert.assertEquals(new LoadFactor(3, 3, 4), result.get(4));
    }

    @Test
    public void getLoadFactorListDropOff1D() throws Exception {
        List<LoadFactor> result = sparkComputationService.getLoadFactorList(
                rowDataset,
                Action.DROPOFF,
                getSeconds("2013-01-03 00:00:00"),
                ONE_DAY_WINDOW);

        checkLoadFactorListDropOff1D(result);
    }


    @Test
    public void getLoadFactorListPickUp1D() throws Exception {
        List<LoadFactor> result = sparkComputationService.getLoadFactorList(
                rowDataset,
                Action.PICKUP,
                getSeconds("2013-01-04 00:00:00"),
                ONE_DAY_WINDOW);

        Assert.assertEquals("Amount of LoadFactors is wrong", 3, result.size());
        Assert.assertEquals(new LoadFactor(1, 1, 9), result.get(0));
        Assert.assertEquals(new LoadFactor(1, 3, 6), result.get(1));
        Assert.assertEquals(new LoadFactor(3, 3, 4), result.get(2));
    }

    @Test
    public void getLoadFactorListDropOff1H() throws Exception {
        List<LoadFactor> result =
                sparkComputationService.getLoadFactorList(
                        rowDataset,
                        Action.DROPOFF,
                        getSeconds("2013-02-03 15:20:00"),
                        ONE_HOUR_WINDOW);

        checkLoadFactorListDropOff1H(result);
    }

    @Test
    public void getLoadFactorListPickUp1H() throws Exception {
        List<LoadFactor> result = sparkComputationService.getLoadFactorList(
                rowDataset,
                Action.PICKUP,
                getSeconds("2013-01-04 02:00:00"),
                ONE_HOUR_WINDOW);

        checkLoadFactorListPickUp1H(result);
    }
}