package uk.dioxic.grib.csv;

import uk.dioxic.grib.model.SimpleBucket;

import java.util.ArrayList;
import java.util.List;

public class SimpleBucketCsv implements Csv<SimpleBucket> {
    @Override
    public String getHeader(List<Integer> parameters) {
        StringBuilder header = new StringBuilder("timestamp,calculationTime,longitude,latitude");

        parameters.forEach(i -> header.append(",").append("parameter").append(i));

        return header.toString();
    }

    @Override
    public String getLine(SimpleBucket record) {
        List<String> fields = new ArrayList<>(List.of(record.getTs().toString(),
                record.getCalcTs().toString(),
                Double.toString(record.getLoc().getLongitude()),
                Double.toString(record.getLoc().getLatitude())));

        record.getParameters().forEach((k, v) -> fields.add(v.toString()));

        return String.join(",", fields);
    }

}
