package oaa.taxi.services;

import java.io.Serializable;
import java.util.List;

import lombok.extern.log4j.Log4j2;
import oaa.taxi.domain.Action;
import oaa.taxi.domain.Fields;
import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
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
import static oaa.taxi.domain.Fields.SECONDS;
import static oaa.taxi.domain.Fields.X;
import static oaa.taxi.domain.Fields.Y;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;

/**
 * Implementation of SparkComputationService that used map function.
 */
@Component("sparkComputationService")
@Log4j2
public class SparkComputationServiceImpl implements SparkComputationService, Serializable {

    private Broadcast<ParametersHolder> parametersBrc;

    @Autowired
    public SparkComputationServiceImpl(Broadcast<ParametersHolder> parametersBrc) {
        this.parametersBrc = parametersBrc;
    }

    @Override
    public List<LoadFactor> getLoadFactorList(Dataset<Row> rowDataset, Action action, long timeInSec, long windowInSec) {
        Dataset<Row> rowDatasetWithoutErrors = rowDataset
            .drop(Fields.Constants.uselessFields)
            .filter(castDouble(IN_X).geq(parametersBrc.getValue().getLeft()).and(castDouble(IN_X).leq(parametersBrc.getValue().getRight())))
            .filter(castDouble(OUT_X).geq(parametersBrc.getValue().getLeft()).and(castDouble(OUT_X).leq(parametersBrc.getValue().getRight())))
            .filter(castDouble(IN_Y).geq(parametersBrc.getValue().getBottom()).and(castDouble(IN_Y).leq(parametersBrc.getValue().getTop())))
            .filter(castDouble(OUT_Y).geq(parametersBrc.getValue().getBottom()).and(castDouble(OUT_Y).leq(parametersBrc.getValue().getTop())));

        //rowDatasetWithoutErrors.show();

        long halfWindowSec = windowInSec / 2;
        Dataset<Row> dsRow = rowDatasetWithoutErrors
            .withColumn(SECONDS.getName(), unix_timestamp(castTimestamp(resolveDateColumn(action))))
            .withColumn(X.getName(), castDouble(resolveXColumn(action)))
            .withColumn(Y.getName(), castDouble(resolveYColumn(action)))
            .drop(Fields.Constants.uselessFields2)
            .filter(col(SECONDS.getName()).geq(timeInSec - halfWindowSec).and(col(SECONDS.getName()).leq(timeInSec + halfWindowSec)));

        //dsRow.show();

        Dataset<LoadFactor> loadFactorDataset = dsRow
            .map((MapFunction<Row, LoadFactor>) this::mapToLoadFactor, Encoders.bean(LoadFactor.class))
            .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
            .agg(sum(col(LoadFactor.VALUE_NAME)).cast(DataTypes.IntegerType).alias(LoadFactor.VALUE_NAME))
            .as(Encoders.bean(LoadFactor.class));
        loadFactorDataset.show();

        return loadFactorDataset.collectAsList();
    }

    private LoadFactor mapToLoadFactor(Row row) {
        double x = row.getAs(X.getName());
        double y = row.getAs(Y.getName());

        final ParametersHolder parametersHolder = parametersBrc.getValue();
        int xp = (int) Math.floor((x - parametersHolder.getLeft()) / parametersHolder.getCellWidth());
        int yp = (int) Math.floor((y - parametersHolder.getBottom()) / parametersHolder.getCellHeight());

        return new LoadFactor(xp, yp, Long.parseLong(row.getAs(PASSENGER_COUNT.getName())));
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
