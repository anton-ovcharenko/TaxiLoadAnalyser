package oaa.taxi.services.impl;

import java.util.List;

import lombok.extern.log4j.Log4j2;
import oaa.taxi.domain.Action;
import oaa.taxi.domain.Fields;
import oaa.taxi.domain.filters.ComputeXIndexFilter;
import oaa.taxi.domain.filters.ComputeYIndexFilter;
import oaa.taxi.domain.models.LoadFactor;
import oaa.taxi.services.LoadAnalyserService;
import oaa.taxi.services.SparkComputationService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

/**
 * @author aovcharenko date 24-05-2017.
 */
@Log4j2
public class CsvSourceLoadAnalyserServiceImpl implements LoadAnalyserService {

    @Value("${load.data.path}")
    private String dataPath;

    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private ComputeXIndexFilter computeXIndexFilter;
    @Autowired
    private ComputeYIndexFilter computeYIndexFilter;
    @Autowired
    @Qualifier("sparkComputationService3")
    private SparkComputationService sparkComputationService;

    @Override
    public List<LoadFactor> getLoadFactors(Action action, long timeInSec, long windowInSec) {
        Dataset<Row> rowDataset = sparkSession.read()
                                              .option("header", "true")
                                              .option("mode", "DROPMALFORMED")
                                              .csv(dataPath)
                                              .drop(Fields.Constants.uselessFields);

        sparkSession.udf().register(ComputeXIndexFilter.NAME, computeXIndexFilter, DataTypes.IntegerType);
        sparkSession.udf().register(ComputeYIndexFilter.NAME, computeYIndexFilter, DataTypes.IntegerType);

        long t1 = System.currentTimeMillis();
        List<LoadFactor> loadFactorList = sparkComputationService.getLoadFactorList(rowDataset, action, timeInSec, windowInSec);
        System.out.format("Process time: %d ms.%nSize: %d.%n", (System.currentTimeMillis() - t1), loadFactorList.size());

        return loadFactorList;
    }
}
