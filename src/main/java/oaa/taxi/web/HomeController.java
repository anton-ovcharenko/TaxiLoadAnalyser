package oaa.taxi.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author aovcharenko date 31-05-2017.
 */
@Controller
public class HomeController {
    @RequestMapping("/")
    public String index() {
        return "index.html";
    }
}
