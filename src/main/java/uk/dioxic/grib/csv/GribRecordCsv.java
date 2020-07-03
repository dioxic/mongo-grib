package uk.dioxic.grib.csv;

import uk.dioxic.grib.model.GribRecord;

import java.util.List;

public class GribRecordCsv implements Csv<GribRecord> {
    @Override
    public String getHeader(List<Integer> parameters) {
        return "timestamp,calculationTime,longitude,latitude,parameter,value";
    }

    @Override
    public String getLine(GribRecord record) {
        return String.join(",", List.of(record.getTs().toString(),
                record.getCalcTs().toString(),
                Double.toString(record.getLoc().getLongitude()),
                Double.toString(record.getLoc().getLatitude()),
                Integer.toString(record.getParameter()),
                Double.toString(record.getValue())));
    }

}
