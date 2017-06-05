package oaa.taxi.web;

import lombok.extern.log4j.Log4j2;
import oaa.taxi.domain.Action;
import oaa.taxi.domain.ParametersHolder;
import oaa.taxi.domain.models.LoadFactorsResponse;
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
    public
    @ResponseBody
    LoadFactorsResponse getLoadFactors(@RequestParam("action") Action action,
                                       @RequestParam("timeInSec") long timeInSec,
                                       @RequestParam("windowInSec") long windowInSec) {

        return LoadFactorsResponse.builder()
                .gridWidth(parametersHolder.getGridWidth())
                .gridHeight(parametersHolder.getGridHeight())
                .loadFactorList(loadAnalyserService.getLoadFactors(action, timeInSec, windowInSec))
                .build();
    }
}
