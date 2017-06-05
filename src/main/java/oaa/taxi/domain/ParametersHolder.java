package oaa.taxi.domain;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;

/**
 * @author aovcharenko date 31-05-2017.
 */
@Component
@Data
@ConfigurationProperties(prefix = "taxi.map")
public class ParametersHolder implements Serializable {
    double left;
    double top;
    double right;
    double bottom;
    int gridWidth;
    int gridHeight;

    double cellWidth;
    double cellHeight;

    @PostConstruct
    public void init() {
        cellWidth = (right - left) / gridWidth;
        cellHeight = (top - bottom) / gridHeight;
    }
}
