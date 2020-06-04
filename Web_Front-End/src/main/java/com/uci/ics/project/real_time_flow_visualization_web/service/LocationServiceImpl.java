package com.uci.ics.project.real_time_flow_visualization_web.service;

import com.uci.ics.project.real_time_flow_visualization_web.dao.LocationDAO;
import com.uci.ics.project.real_time_flow_visualization_web.entity.Location;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class LocationServiceImpl implements LocationService {

    private LocationDAO locationDAO;

    @Autowired
    public LocationServiceImpl(LocationDAO locationDAO) {
        this.locationDAO = locationDAO;
    }

    @Override
    @Transactional
    public List<Location> findAll() {
        return locationDAO.findAll();
    }
}
