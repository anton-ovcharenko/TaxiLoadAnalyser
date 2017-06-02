package oaa.taxi.web;

import java.util.List;

import javafx.util.Pair;
import lombok.extern.log4j.Log4j2;
import oaa.taxi.domain.Action;
import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.models.LoadFactor;
import oaa.taxi.services.LoadAnalyserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

/**
 * @author aovcharenko date 24-05-2017.
 */
@Controller
@Log4j2
public class MainController {

    @Autowired
    private LoadAnalyserService loadAnalyserService;

    @Autowired
    private ParametersHolder parametersHolder;

    @RequestMapping("/")
    public String index() {
        return "index.html";
    }

    @RequestMapping(method = GET, value = "/getLoadFactors")
    public @ResponseBody
    Pair<Pair, List<LoadFactor>> getLoadFactors(@RequestParam("action") Action action,
                                                @RequestParam("timeInSec") long timeInSec,
                                                @RequestParam("windowInSec") long windowInSec) {
        return new Pair<>(new Pair<>(parametersHolder.getGridWidth(), parametersHolder.getGridHeight()),
                          loadAnalyserService.getLoadFactors(action, timeInSec, windowInSec)
        );
    }
}
