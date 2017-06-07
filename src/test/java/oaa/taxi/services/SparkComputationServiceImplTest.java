package oaa.taxi.services;

import java.util.List;

import oaa.taxi.domain.Action;
import oaa.taxi.domain.models.LoadFactor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author aovcharenko date 01-06-2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class SparkComputationServiceImplTest extends BaseSparkComputationServiceTest {

    private SparkComputationServiceImpl sparkComputationService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        sparkComputationService = new SparkComputationServiceImpl(createBroadcastParameterHolder());
    }

    @Test
    public void getLoadFactorListDropOff4D() throws Exception {
        List<LoadFactor> result = sparkComputationService.getLoadFactorList(
            rowDataset,
            Action.DROPOFF,
            getSeconds("2013-01-02 00:00:00"),
            FOUR_DAY_WINDOW);

        checkLoadFactorListDropOff4D(result);
    }

    @Test
    public void getLoadFactorListPickUp4D() throws Exception {
        List<LoadFactor> result = sparkComputationService.getLoadFactorList(
            rowDataset,
            Action.PICKUP,
            getSeconds("2013-01-02 00:00:00"),
            FOUR_DAY_WINDOW);

        checkLoadFactorListPickUp4D(result);
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

        checkLoadFactorListPickUp1D(result);
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