package ex3;


import java.io.Serializable;

public class StationRecord implements Serializable {

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

}
