package com.uci.ics.project.real_time_flow_visualization_web.controller;

import com.uci.ics.project.real_time_flow_visualization_web.entity.Location;
import com.uci.ics.project.real_time_flow_visualization_web.service.LocationService;
import net.sf.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;

@Controller
@RequestMapping("/map")
public class LocationController {

    private LocationService locationService;

    @Autowired
    public LocationController(LocationService locationService) {
        this.locationService = locationService;
    }

//    @GetMapping("/visitors")
//    public List<Location> findAll() {
//        return locationService.findAll();
//    }

    @GetMapping("/visitors")
    public String showVisitorFlow(Model model) {
        List<Location> locations = locationService.findAll();
        JSONArray jsonArray = JSONArray.fromObject(locations);
        System.out.println(jsonArray);
        model.addAttribute("location_json", jsonArray);
        return "VisitorsFlowMap";
    }
}
