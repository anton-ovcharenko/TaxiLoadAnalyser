package oaa.taxi.services;

import java.util.List;

import lombok.extern.log4j.Log4j2;
import oaa.taxi.domain.Action;
import oaa.taxi.domain.filters.ComputeXIndexFilter;
import oaa.taxi.domain.filters.ComputeYIndexFilter;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static oaa.taxi.domain.Fields.HACK_LICENSE;
import static oaa.taxi.domain.Fields.IN_TS;
import static oaa.taxi.domain.Fields.IN_X;
import static oaa.taxi.domain.Fields.IN_Y;
import static oaa.taxi.domain.Fields.MEDALLION;
import static oaa.taxi.domain.Fields.OUT_TS;
import static oaa.taxi.domain.Fields.OUT_X;
import static oaa.taxi.domain.Fields.OUT_Y;
import static oaa.taxi.domain.Fields.PASSENGER_COUNT;
import static oaa.taxi.domain.Fields.RATE_CODE;
import static oaa.taxi.domain.Fields.STORE_AND_FWD_FLAG;
import static oaa.taxi.domain.Fields.TRIP_DISTANCE;
import static oaa.taxi.domain.Fields.TRIP_TIME_IN_SECS;
import static oaa.taxi.domain.Fields.VENDOR_ID;

/**
 * @author aovcharenko date 24-05-2017.
 */
@Component
@Log4j2
public class LoadAnalyserService {

    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private ComputeXIndexFilter computeXIndexFilter;
    @Autowired
    private ComputeYIndexFilter computeYIndexFilter;
    @Autowired
    private SparkComputationService sparkComputationService;

    public List<LoadFactor> getLoadFactors(Action action, long timeInSec, long windowInSec) {
        Dataset<Row> rowDataset = sparkSession
            .read()
            .format("CSV")
            .option("header", "true")
            .schema(generateSchema())
            //.load("d:/programming/PROJECT/IDEA_WS/Taxi/data/trip_data/trip_data_1.csv")
            //.load("d:/programming/PROJECT/IDEA_WS/Taxi/data/trip_data/*.csv")
            .load("./data/small.csv")
            ;

        sparkSession.udf().register(ComputeXIndexFilter.NAME, computeXIndexFilter, DataTypes.IntegerType);
        sparkSession.udf().register(ComputeYIndexFilter.NAME, computeYIndexFilter, DataTypes.IntegerType);

        long t1 = System.currentTimeMillis();
        List<LoadFactor> loadFactorList = sparkComputationService.getLoadFactorList(rowDataset, action, timeInSec, windowInSec);
        System.out.format("Process time: %d ms.%nSize: %d.%n", (System.currentTimeMillis() - t1), loadFactorList.size());

        return loadFactorList;
    }

    public static StructType generateSchema() {
        return new StructType(new StructField[] {
            DataTypes.createStructField(MEDALLION.getName(), DataTypes.StringType, false),
            DataTypes.createStructField(HACK_LICENSE.getName(), DataTypes.StringType, false),
            DataTypes.createStructField(VENDOR_ID.getName(), DataTypes.StringType, false),
            DataTypes.createStructField(RATE_CODE.getName(), DataTypes.StringType, false),
            DataTypes.createStructField(STORE_AND_FWD_FLAG.getName(), DataTypes.StringType, false),
            DataTypes.createStructField(IN_TS.getName(), DataTypes.TimestampType, false),
            DataTypes.createStructField(OUT_TS.getName(), DataTypes.TimestampType, false),
            DataTypes.createStructField(PASSENGER_COUNT.getName(), DataTypes.IntegerType, false),
            DataTypes.createStructField(TRIP_TIME_IN_SECS.getName(), DataTypes.IntegerType, false),
            DataTypes.createStructField(TRIP_DISTANCE.getName(), DataTypes.DoubleType, false),
            DataTypes.createStructField(IN_X.getName(), DataTypes.DoubleType, false),
            DataTypes.createStructField(IN_Y.getName(), DataTypes.DoubleType, false),
            DataTypes.createStructField(OUT_X.getName(), DataTypes.DoubleType, false),
            DataTypes.createStructField(OUT_Y.getName(), DataTypes.DoubleType, false)
        });
    }
}