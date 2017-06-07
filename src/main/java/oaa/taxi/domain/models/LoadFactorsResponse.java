package oaa.taxi.domain.models;

import java.util.List;

import lombok.Builder;
import lombok.Data;

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
