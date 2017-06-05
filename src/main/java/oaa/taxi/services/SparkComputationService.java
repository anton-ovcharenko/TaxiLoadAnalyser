package oaa.taxi.services;

import oaa.taxi.domain.Action;
import oaa.taxi.domain.models.LoadFactor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Created by Antman on 02.06.2017.
 */
public interface SparkComputationService {
    List<LoadFactor> getLoadFactorList(Dataset<Row> rowDataset, Action action, long timeInSec, long windowInSec);
}
