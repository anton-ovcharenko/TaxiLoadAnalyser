package oaa.taxi.domain.filters;

import oaa.taxi.domain.ParametersHolder;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author aovcharenko date 02-06-2017.
 */
@Component
public class ComputeXIndexFilter implements UDF1<Double, Integer> {
    public static final String NAME = "getXIndex";

    private Broadcast<ParametersHolder> parametersBrc;

    @Autowired
    public ComputeXIndexFilter(Broadcast<ParametersHolder> parametersBrc) {
        this.parametersBrc = parametersBrc;
    }

    @Override
    public Integer call(Double x) throws Exception {
        return (int) Math.floor((x.doubleValue() - parametersBrc.getValue().getLeft()) / parametersBrc.getValue().getCellWidth());
    }
}
