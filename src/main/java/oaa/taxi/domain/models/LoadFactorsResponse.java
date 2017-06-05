package oaa.taxi.domain.models;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Created by Antman on 05.06.2017.
 */
@Builder
@Data
public class LoadFactorsResponse {
    int gridWidth;
    int gridHeight;
    List<LoadFactor> loadFactorList;
}
