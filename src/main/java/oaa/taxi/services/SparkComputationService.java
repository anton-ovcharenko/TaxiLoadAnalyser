package oaa.taxi.services;

import java.util.List;

import oaa.taxi.domain.Action;
import oaa.taxi.domain.Fields;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

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
}
