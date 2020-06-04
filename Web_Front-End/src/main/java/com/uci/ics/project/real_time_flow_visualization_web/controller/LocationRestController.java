package com.uci.ics.project.real_time_flow_visualization_web.controller;

import com.uci.ics.project.real_time_flow_visualization_web.entity.Location;
import com.uci.ics.project.real_time_flow_visualization_web.service.LocationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/map")
public class LocationRestController {

    private LocationService locationService;

    @Autowired
    public LocationRestController(LocationService locationService) {
        this.locationService = locationService;
    }

    @GetMapping("/visitors")
    public List<Location> findAll() {
        return locationService.findAll();
    }
}
