package oaa.taxi.services;

import java.io.Serializable;
import java.util.List;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static oaa.taxi.domain.Fields.IN_SECONDS;
import static oaa.taxi.domain.Fields.IN_TS;
import static oaa.taxi.domain.Fields.IN_X;
import static oaa.taxi.domain.Fields.IN_X_INDEX;
import static oaa.taxi.domain.Fields.IN_Y;
import static oaa.taxi.domain.Fields.IN_Y_INDEX;
import static oaa.taxi.domain.Fields.OUT_SECONDS;
import static oaa.taxi.domain.Fields.OUT_TS;
import static oaa.taxi.domain.Fields.OUT_X;
import static oaa.taxi.domain.Fields.OUT_X_INDEX;
import static oaa.taxi.domain.Fields.OUT_Y;
import static oaa.taxi.domain.Fields.OUT_Y_INDEX;
import static oaa.taxi.domain.Fields.PASSENGER_COUNT;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;

/**
 * Implementation of SparkComputationService that used won UDF functions.
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
        Dataset<Row> rowDatasetWithoutErrors = rowDataset
            .drop(Fields.Constants.uselessFields)
            //filter wrong (out of map points)
            .filter(castDouble(IN_X).geq(parametersBrc.getValue().getLeft()).and(castDouble(IN_X).leq(parametersBrc.getValue().getRight())))
            .filter(castDouble(OUT_X).geq(parametersBrc.getValue().getLeft()).and(castDouble(OUT_X).leq(parametersBrc.getValue().getRight())))
            .filter(castDouble(IN_Y).geq(parametersBrc.getValue().getBottom()).and(castDouble(IN_Y).leq(parametersBrc.getValue().getTop())))
            .filter(castDouble(OUT_Y).geq(parametersBrc.getValue().getBottom()).and(castDouble(OUT_Y).leq(parametersBrc.getValue().getTop())))
            //add auxiliary columns
            .withColumn(IN_SECONDS.getName(), unix_timestamp(castTimestamp(IN_TS)))
            .withColumn(OUT_SECONDS.getName(), unix_timestamp(castTimestamp(OUT_TS)))
            .withColumn(IN_X_INDEX.getName(), callUDF(ComputeXIndexFilter.NAME, castDouble(IN_X)))
            .withColumn(IN_Y_INDEX.getName(), callUDF(ComputeYIndexFilter.NAME, castDouble(IN_Y)))
            .withColumn(OUT_X_INDEX.getName(), callUDF(ComputeXIndexFilter.NAME, castDouble(OUT_X)))
            .withColumn(OUT_Y_INDEX.getName(), callUDF(ComputeYIndexFilter.NAME, castDouble(OUT_Y)))
            .drop(Fields.Constants.uselessFields2)
            //.persist(StorageLevel.MEMORY_AND_DISK())
            ;

        long halfWindow = windowInSec / 2;
        String secondColName = resolveSecondsColumn(action).getName();
        Dataset<LoadFactor> loadFactorDataset = rowDatasetWithoutErrors
            .filter(col(secondColName).geq(timeInSec - halfWindow).and(col(secondColName).leq(timeInSec + halfWindow)))
            .withColumn(LoadFactor.X_INDEX_NAME, castInt(resolveXIndexColumn(action)))
            .withColumn(LoadFactor.Y_INDEX_NAME, castInt(resolveYIndexColumn(action)))
            .withColumn(LoadFactor.VALUE_NAME, castLong(PASSENGER_COUNT))
            .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
            .agg(sum(col(LoadFactor.VALUE_NAME)).cast(DataTypes.LongType).alias(LoadFactor.VALUE_NAME))
            .as(Encoders.bean(LoadFactor.class));

        List<LoadFactor> result = loadFactorDataset
            .collectAsList();

        return result;
    }
}
