package uk.dioxic.grib.csv;

import java.util.List;

public interface Csv<T> {

    String getHeader(List<Integer> parameters);

    String getLine(T record);

}
