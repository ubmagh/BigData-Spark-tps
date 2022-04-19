package ex3;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

public class StationRecord implements Serializable, Comparable {

    private String id;
    private String recordType; // min max ..
    private Double temperature;
    private String date;

    public StationRecord() {
    }

    public StationRecord(String id, String recordType, Double temperature, String date) {
        this.id = id;
        this.recordType = recordType;
        this.temperature = temperature;
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StationRecord that = (StationRecord) o;
        return Objects.equals(id, that.id) && Objects.equals(recordType, that.recordType) && Objects.equals(temperature, that.temperature) && Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, recordType, temperature, date);
    }

    @Override
    public int compareTo(Object o) {
        StationRecord sr = (StationRecord) o;
        return Double.compare( this.getTemperature(), sr.getTemperature());
    }
}
