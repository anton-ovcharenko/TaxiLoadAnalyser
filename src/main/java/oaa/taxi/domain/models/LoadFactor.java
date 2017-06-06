package oaa.taxi.domain.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author aovcharenko date 31-05-2017.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoadFactor {
    public final static String X_INDEX_NAME = "xIndex";
    public final static String Y_INDEX_NAME = "yIndex";
    public final static String VALUE_NAME = "value";

    int xIndex;
    int yIndex;
    long value;
}
