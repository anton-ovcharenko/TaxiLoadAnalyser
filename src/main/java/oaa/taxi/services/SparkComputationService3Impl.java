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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static oaa.taxi.domain.Fields.IN_TS;
import static oaa.taxi.domain.Fields.IN_X;
import static oaa.taxi.domain.Fields.IN_Y;
import static oaa.taxi.domain.Fields.OUT_TS;
import static oaa.taxi.domain.Fields.OUT_X;
import static oaa.taxi.domain.Fields.OUT_Y;
import static oaa.taxi.domain.Fields.PASSENGER_COUNT;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.floor;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;

/**
 * @author aovcharenko date 01-06-2017.
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
        String xColName = resolveXColumn(action);
        String yColName = resolveYColumn(action);
        String dateColName = resolveDateColumn(action);
        ParametersHolder parametersHolder = parametersBrc.getValue();

        Dataset<Row> filteredDataset = rowDataset
            .drop(Action.PICKUP == action ? Fields.Constants.uselessFields3_out : Fields.Constants.uselessFields3_in)
            .filter(unix_timestamp(col(dateColName)).between(timeInSec - halfWindowSec, timeInSec + halfWindowSec))
            .filter(col(xColName).between(parametersHolder.getLeft(), parametersHolder.getRight())
                                 .and(col(yColName).between(parametersHolder.getBottom(), parametersHolder.getTop())))
            ;

        //filteredDataset.show();

        Dataset<LoadFactor> loadFactorDataset = filteredDataset

//            .withColumn(LoadFactor.X_INDEX_NAME,
//                        floor((col(xColName).minus(parametersHolder.getLeft())).divide(parametersHolder.getCellWidth())).cast(DataTypes.IntegerType))
//            .withColumn(LoadFactor.Y_INDEX_NAME,
//                        floor((col(yColName).minus(parametersHolder.getBottom())).divide(parametersHolder.getCellHeight())).cast(DataTypes.IntegerType))

            .withColumn(LoadFactor.X_INDEX_NAME, callUDF(ComputeXIndexFilter.NAME, col(xColName)))
            .withColumn(LoadFactor.Y_INDEX_NAME, callUDF(ComputeYIndexFilter.NAME, col(yColName)))

            .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
            .agg(sum(col(PASSENGER_COUNT.getName())).alias(LoadFactor.VALUE_NAME))

            .as(Encoders.bean(LoadFactor.class))
            ;

        //loadFactorDataset.show();

        return loadFactorDataset.collectAsList();
    }

    private String resolveDateColumn(Action action) {
        return Action.PICKUP == action ? IN_TS.getName() : OUT_TS.getName();
    }

    private String resolveXColumn(Action action) {
        return Action.PICKUP == action ? IN_X.getName() : OUT_X.getName();
    }

    private String resolveYColumn(Action action) {
        return Action.PICKUP == action ? IN_Y.getName() : OUT_Y.getName();
    }
}
