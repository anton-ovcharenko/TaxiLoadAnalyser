package oaa.taxi.services;

import java.util.List;

import oaa.taxi.domain.Action;
import oaa.taxi.domain.Fields;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

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
import static org.apache.spark.sql.functions.col;

/**
 * Created by Antman on 02.06.2017.
 */
public interface SparkComputationService {
    List<LoadFactor> getLoadFactorList(Dataset<Row> rowDataset, Action action, long timeInSec, long windowInSec);

    default Column castDouble(Fields field) {
        return col(field.getName()).cast(DataTypes.DoubleType);
    }

    default Column castTimestamp(Fields field) {
        return col(field.getName()).cast(DataTypes.TimestampType);
    }

    default Column castLong(Fields field) {
        return col(field.getName()).cast(DataTypes.LongType);
    }

    default Column castInt(Fields field) {
        return col(field.getName()).cast(DataTypes.IntegerType);
    }

    default Fields resolveSecondsColumn(Action action) {
        return Action.PICKUP == action ? IN_SECONDS : OUT_SECONDS;
    }

    default Fields resolveXIndexColumn(Action action) {
        return Action.PICKUP == action ? IN_X_INDEX : OUT_X_INDEX;
    }

    default Fields resolveYIndexColumn(Action action) {
        return Action.PICKUP == action ? IN_Y_INDEX : OUT_Y_INDEX;
    }

    default Fields resolveDateColumn(Action action) {
        return Action.PICKUP == action ? IN_TS : OUT_TS;
    }

    default Fields resolveXColumn(Action action) {
        return Action.PICKUP == action ? IN_X : OUT_X;
    }

    default Fields resolveYColumn(Action action) {
        return Action.PICKUP == action ? IN_Y : OUT_Y;
    }
}
