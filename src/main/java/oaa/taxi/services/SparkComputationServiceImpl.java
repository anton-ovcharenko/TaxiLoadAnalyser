package oaa.taxi.services;

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
@Component("sparkComputationServiceImpl")
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

    private LoadFactor mapToLoadFactor(Row row) {
        double x = row.getAs(X.getName());
        double y = row.getAs(Y.getName());

        int xp = (int) Math.floor((x - parametersBrc.getValue().getLeft()) / parametersBrc.getValue().getCellWidth());
        int yp = (int) Math.floor((y - parametersBrc.getValue().getBottom()) / parametersBrc.getValue().getCellHeight());

        return new LoadFactor(xp, yp, row.getAs(PASSENGER_COUNT.getName()));
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
