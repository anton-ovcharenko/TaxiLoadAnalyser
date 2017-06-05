package oaa.taxi.services;

import lombok.extern.log4j.Log4j2;
import oaa.taxi.domain.Action;
import oaa.taxi.domain.Fields;
import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.filters.ComputeXIndexFilter;
import oaa.taxi.domain.filters.ComputeYIndexFilter;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

import static oaa.taxi.domain.Fields.*;
import static org.apache.spark.sql.functions.*;

/**
 * @author aovcharenko date 01-06-2017.
 */
@Component("sparkComputationService2")
@Log4j2
public class SparkComputationService2Impl implements SparkComputationService, Serializable {

    private Broadcast<ParametersHolder> parametersBrc;

    @Autowired
    public SparkComputationService2Impl(Broadcast<ParametersHolder> parametersBrc) {
        this.parametersBrc = parametersBrc;
    }

    @Override
    public List<LoadFactor> getLoadFactorList(Dataset<Row> rowDataset, Action action, long timeInSec, long windowInSec) {
        long t1 = System.currentTimeMillis();
        Dataset<Row> rowDatasetWithoutErrors = rowDataset
                .drop(Fields.Constants.uselessFields)
                .filter(col(IN_X.getName()).geq(parametersBrc.getValue().getLeft()).and(col(IN_X.getName()).leq(parametersBrc.getValue().getRight())))
                .filter(col(OUT_X.getName()).geq(parametersBrc.getValue().getLeft()).and(col(OUT_X.getName()).leq(parametersBrc.getValue().getRight())))
                .filter(col(IN_Y.getName()).geq(parametersBrc.getValue().getBottom()).and(col(IN_Y.getName()).leq(parametersBrc.getValue().getTop())))
                .filter(col(OUT_Y.getName()).geq(parametersBrc.getValue().getBottom()).and(col(OUT_Y.getName()).leq(parametersBrc.getValue().getTop())))
                .withColumn(IN_SECONDS.getName(), unix_timestamp(col(IN_TS.getName())))
                .withColumn(OUT_SECONDS.getName(), unix_timestamp(col(OUT_TS.getName())))
                .withColumn(IN_X_INDEX.getName(), callUDF(ComputeXIndexFilter.NAME, col(IN_X.getName())))
                .withColumn(IN_Y_INDEX.getName(), callUDF(ComputeYIndexFilter.NAME, col(IN_Y.getName())))
                .withColumn(OUT_X_INDEX.getName(), callUDF(ComputeXIndexFilter.NAME, col(OUT_X.getName())))
                .withColumn(OUT_Y_INDEX.getName(), callUDF(ComputeYIndexFilter.NAME, col(OUT_Y.getName())))
                .drop(Fields.Constants.uselessFields2)
                .persist(StorageLevel.MEMORY_AND_DISK());

        long t2 = System.currentTimeMillis();
        final long halfWindow = windowInSec / 2;
        final String secondColName = resolveSecondsColumn(action);
        Dataset<LoadFactor> loadFactorDataset = rowDatasetWithoutErrors
                .filter(col(secondColName).geq(timeInSec - halfWindow).and(col(secondColName).leq(timeInSec + halfWindow)))

                //.map((MapFunction<Row, LoadFactor>) (r) -> mapToLoadFactor2(r, action), Encoders.bean(LoadFactor.class))

                .withColumn(LoadFactor.X_INDEX_NAME, col(resolveXIndexColumn(action)))
                .withColumn(LoadFactor.Y_INDEX_NAME, col(resolveYIndexColumn(action)))
                .withColumn(LoadFactor.VALUE_NAME, col(PASSENGER_COUNT.getName()))

                .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
                .agg(sum(col(LoadFactor.VALUE_NAME)).cast(DataTypes.IntegerType).alias(LoadFactor.VALUE_NAME))
                .orderBy(col(LoadFactor.VALUE_NAME).desc())
                .as(Encoders.bean(LoadFactor.class));

        long t3 = System.currentTimeMillis();
        List<LoadFactor> result = loadFactorDataset
                .collectAsList();

        long t4 = System.currentTimeMillis();
        log.debug("time1: {}", t2 - t1);
        log.debug("time2: {}", t3 - t2);
        log.debug("time3: {}", t4 - t3);

        return result;
    }

    private String resolveSecondsColumn(Action action) {
        return Action.PICKUP == action ? IN_SECONDS.getName() : OUT_SECONDS.getName();
    }

    private String resolveXIndexColumn(Action action) {
        return Action.PICKUP == action ? IN_X_INDEX.getName() : OUT_X_INDEX.getName();
    }

    private String resolveYIndexColumn(Action action) {
        return Action.PICKUP == action ? IN_Y_INDEX.getName() : OUT_Y_INDEX.getName();
    }

}
