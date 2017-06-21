package oaa.taxi.services.impl;

import java.io.Serializable;
import java.util.List;

import lombok.extern.log4j.Log4j2;
import oaa.taxi.domain.Action;
import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.filters.ComputeXIndexFilter;
import oaa.taxi.domain.filters.ComputeYIndexFilter;
import oaa.taxi.domain.models.LoadFactor;
import oaa.taxi.services.SparkComputationService;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static oaa.taxi.domain.Fields.PASSENGER_COUNT;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;

/**
 * Most optimal implementation of SparkComputationService that used either UDF function or  transformation based on simple math operation.
 */
@Component("sparkComputationService3")
@Log4j2
public class SparkComputationService3Impl implements SparkComputationService, Serializable {

    private Broadcast<ParametersHolder> parametersBrc;

    @Autowired
    public SparkComputationService3Impl(Broadcast<ParametersHolder> parametersBrc) {
        this.parametersBrc = parametersBrc;
    }

    @Override
    public List<LoadFactor> getLoadFactorList(Dataset<Row> rowDataset, Action action, long timeInSec, long windowInSec) {
        long halfWindowSec = windowInSec / 2;
        ParametersHolder parametersHolder = parametersBrc.getValue();

        Column castedXCol = castDouble(resolveXColumn(action));
        Column castedYCol = castDouble(resolveYColumn(action));
        Column castedDateCol = castTimestamp(resolveDateColumn(action));

        Dataset<Row> filteredDataset = rowDataset
            //.drop(Fields.Constants.uselessFields)
            //.drop(Action.PICKUP == action ? Fields.Constants.uselessFields3_out : Fields.Constants.uselessFields3_in)
            .filter(unix_timestamp(castedDateCol).between(timeInSec - halfWindowSec, timeInSec + halfWindowSec)
                                                 .and(castedXCol.between(parametersHolder.getLeft(), parametersHolder.getRight()))
                                                 .and(castedYCol.between(parametersHolder.getBottom(), parametersHolder.getTop())));

        //filteredDataset.show();

        Dataset<LoadFactor> loadFactorDataset = filteredDataset
            .withColumn(LoadFactor.X_INDEX_NAME, functions.callUDF(ComputeXIndexFilter.NAME, castedXCol))
            .withColumn(LoadFactor.Y_INDEX_NAME, functions.callUDF(ComputeYIndexFilter.NAME, castedYCol))

//            .withColumn(LoadFactor.X_INDEX_NAME,
//                        floor((castedXCol.minus(parametersHolder.getLeft())).divide(parametersHolder.getCellWidth())).cast(DataTypes.IntegerType))
//            .withColumn(LoadFactor.Y_INDEX_NAME,
//                        floor((castedYCol.minus(parametersHolder.getBottom())).divide(parametersHolder.getCellHeight())).cast(DataTypes.IntegerType))

            .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
            .agg(sum(castLong(PASSENGER_COUNT)).alias(LoadFactor.VALUE_NAME))
            .as(Encoders.bean(LoadFactor.class));

        //loadFactorDataset.show();

        return loadFactorDataset.collectAsList();
    }
}
