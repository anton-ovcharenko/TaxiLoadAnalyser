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
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
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
import static oaa.taxi.domain.Fields.SECONDS;
import static oaa.taxi.domain.Fields.X;
import static oaa.taxi.domain.Fields.Y;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;

/**
 * @author aovcharenko date 01-06-2017.
 */
@Component
@Log4j2
public class SparkComputationService implements Serializable {

    private Broadcast<ParametersHolder> parametersBrc;

    @Autowired
    public SparkComputationService(Broadcast<ParametersHolder> parametersBrc) {
        this.parametersBrc = parametersBrc;
    }

    public List<LoadFactor> getLoadFactorList_first(Dataset<Row> rowDataset, Action action, long timeInSec, long windowInSec) {
        Dataset<Row> rowDatasetWithoutErrors = rowDataset
            .drop(Fields.Constants.uselessFields)
            .filter(col(IN_X.getName()).geq(parametersBrc.getValue().getLeft()).and(col(IN_X.getName()).leq(parametersBrc.getValue().getRight())))
            .filter(col(OUT_X.getName()).geq(parametersBrc.getValue().getLeft()).and(col(OUT_X.getName()).leq(parametersBrc.getValue().getRight())))
            .filter(col(IN_Y.getName()).geq(parametersBrc.getValue().getBottom()).and(col(IN_Y.getName()).leq(parametersBrc.getValue().getTop())))
            .filter(col(OUT_Y.getName()).geq(parametersBrc.getValue().getBottom()).and(col(OUT_Y.getName()).leq(parametersBrc.getValue().getTop())))
            .persist(StorageLevel.MEMORY_AND_DISK());

        //rowDatasetWithoutErrors.show();

        long halfWindowSec = windowInSec / 2;
        Dataset<Row> dsRow = rowDatasetWithoutErrors
            .withColumn(SECONDS.getName(), unix_timestamp(rowDatasetWithoutErrors.col(resolveDateColumn(action))))
            .withColumn(X.getName(), rowDatasetWithoutErrors.col(resolveXColumn(action)))
            .withColumn(Y.getName(), rowDatasetWithoutErrors.col(resolveYColumn(action)))
            .drop(Fields.Constants.uselessFields2)
            .filter(col(SECONDS.getName()).geq(timeInSec - halfWindowSec).and(col(SECONDS.getName()).leq(timeInSec + halfWindowSec)));

        //dsRow.show();

        Dataset<LoadFactor> loadFactorDataset = dsRow
            .map((MapFunction<Row, LoadFactor>) this::mapToLoadFactor, Encoders.bean(LoadFactor.class))
            .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
            .agg(sum(col(LoadFactor.VALUE_NAME)).cast(DataTypes.IntegerType).alias(LoadFactor.VALUE_NAME))
            .orderBy(col(LoadFactor.VALUE_NAME).desc())
            .as(Encoders.bean(LoadFactor.class));
        loadFactorDataset.show();

        return loadFactorDataset.collectAsList();
    }

    public List<LoadFactor> getLoadFactorList(Dataset<Row> rowDataset, Action action, long timeInSec, long windowInSec) {
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
            .persist(StorageLevel.MEMORY_ONLY());

        final long halfWindow = windowInSec / 2;
        final String secondColName = resolveSecondsColumn(action);
        Dataset<LoadFactor> loadFactorDataset = rowDatasetWithoutErrors
            .filter(col(secondColName).geq(timeInSec - halfWindow).and(col(secondColName).leq(timeInSec + halfWindow)))
            .map((MapFunction<Row, LoadFactor>) (r) -> mapToLoadFactor2(r, action), Encoders.bean(LoadFactor.class))
            .groupBy(col(LoadFactor.X_INDEX_NAME), col(LoadFactor.Y_INDEX_NAME))
            .agg(sum(col(LoadFactor.VALUE_NAME)).cast(DataTypes.IntegerType).alias(LoadFactor.VALUE_NAME))
            .orderBy(col(LoadFactor.VALUE_NAME).desc())
            .as(Encoders.bean(LoadFactor.class));
        return loadFactorDataset.collectAsList();
    }

    private LoadFactor mapToLoadFactor(Row row) {
        double x = row.getAs(X.getName());
        double y = row.getAs(Y.getName());

        int xp = (int) Math.floor((x - parametersBrc.getValue().getLeft()) / parametersBrc.getValue().getCellWidth());
        int yp = (int) Math.floor((y - parametersBrc.getValue().getBottom()) / parametersBrc.getValue().getCellHeight());

        return new LoadFactor(xp, yp, row.getAs(PASSENGER_COUNT.getName()));
    }

    private LoadFactor mapToLoadFactor2(Row row, Action action) {
        return new LoadFactor(row.getAs(resolveXIndexColumn(action)),
                              row.getAs(resolveYIndexColumn(action)),
                              row.getAs(PASSENGER_COUNT.getName()));
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
