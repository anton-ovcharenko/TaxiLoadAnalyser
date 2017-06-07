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
        Fields xCol = resolveXColumn(action);
        Fields yCol = resolveYColumn(action);
        Fields dateCol = resolveDateColumn(action);
        ParametersHolder parametersHolder = parametersBrc.getValue();

        Dataset<Row> filteredDataset = rowDataset
            .drop(Action.PICKUP == action ? Fields.Constants.uselessFields3_out : Fields.Constants.uselessFields3_in)
            .filter(unix_timestamp(castTimestamp(dateCol)).between(timeInSec - halfWindowSec, timeInSec + halfWindowSec))
            .filter(castDouble(xCol).between(parametersHolder.getLeft(), parametersHolder.getRight())
                                    .and(castDouble(yCol).between(parametersHolder.getBottom(), parametersHolder.getTop())));

        //filteredDataset.show();

        Dataset<LoadFactor> loadFactorDataset = filteredDataset
            .withColumn(LoadFactor.X_INDEX_NAME, callUDF(ComputeXIndexFilter.NAME, castDouble(xCol)))
            .withColumn(LoadFactor.Y_INDEX_NAME, callUDF(ComputeYIndexFilter.NAME, castDouble(yCol)))

            .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
            .agg(sum(castLong(PASSENGER_COUNT)).alias(LoadFactor.VALUE_NAME))

            .as(Encoders.bean(LoadFactor.class));

        //loadFactorDataset.show();

        return loadFactorDataset.collectAsList();
    }

    private Fields resolveDateColumn(Action action) {
        return Action.PICKUP == action ? IN_TS : OUT_TS;
    }

    private Fields resolveXColumn(Action action) {
        return Action.PICKUP == action ? IN_X : OUT_X;
    }

    private Fields resolveYColumn(Action action) {
        return Action.PICKUP == action ? IN_Y : OUT_Y;
    }
}
