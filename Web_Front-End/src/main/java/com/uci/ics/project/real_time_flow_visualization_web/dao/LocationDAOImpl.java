package com.uci.ics.project.real_time_flow_visualization_web.dao;

import com.uci.ics.project.real_time_flow_visualization_web.entity.Location;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Repository
public class LocationDAOImpl implements LocationDAO{

    private StringRedisTemplate stringRedisTemplate;

    private Properties properties;

    @Autowired
    public LocationDAOImpl(StringRedisTemplate stringRedisTemplate) throws IOException {
        this.stringRedisTemplate = stringRedisTemplate;
        // Load the property file.
        InputStream inputStream = getClass().getClassLoader().
                getResourceAsStream("config.properties");
        properties = new Properties();
        assert inputStream != null;
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        properties.load(inputStreamReader);
    }

    @Override
    public List<Location> findAll() {
        Set<String> keys = stringRedisTemplate.keys("*");
        if(keys == null) return null;
        Iterator<String> iterator = keys.iterator();
        List<Location> locations = new ArrayList<>(2000);
        while(iterator.hasNext()) {
            String key = iterator.next();
            String[] temp = key.split(",");
            double latitude = Double.parseDouble(temp[0]);
            double longitude = Double.parseDouble(temp[1]);
            String value = stringRedisTemplate.opsForValue().get(key);
            if(value == null) continue;
            int count = Integer.parseInt(value);
            if(count < Integer.parseInt(properties.getProperty("threshold"))) continue;
            Location location = new Location(latitude, longitude, count);
            locations.add(location);
        }
        return locations;
    }
}
