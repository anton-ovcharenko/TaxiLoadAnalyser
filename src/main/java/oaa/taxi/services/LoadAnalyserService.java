package oaa.taxi.services;

import java.util.List;

import oaa.taxi.domain.Action;
import oaa.taxi.domain.models.LoadFactor;

/**
 * @author aovcharenko date 09-06-2017.
 */
public interface LoadAnalyserService {
    List<LoadFactor> getLoadFactors(Action action, long timeInSec, long windowInSec);
}
