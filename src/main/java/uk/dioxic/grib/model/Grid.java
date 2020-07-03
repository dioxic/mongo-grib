package uk.dioxic.grib.model;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public enum Grid {
    EUROPE(-23,39,36,72),
    ONE_POINT(12, 12.25, 55, 55.25),
    FOUR_POINT(12, 12.50, 55, 55.50),
    DENMARK(8, 12, 55, 57),
    WIDE(0, 20, 45, 60);

    @Getter
    private final List<Point> points;

    Grid(double minLongitude, double maxLongitude, double minLatitude, double maxLatitude) {
        points = new ArrayList<>();

        for (double longitude = minLongitude; longitude < maxLongitude; longitude = longitude + 0.25d) {
            for (double latitude = minLatitude; latitude < maxLatitude; latitude = latitude + 0.25d) {
                points.add(new Point(longitude, latitude));
            }
        }
    }

}
