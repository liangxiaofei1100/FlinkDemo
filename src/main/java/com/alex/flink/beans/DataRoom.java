package com.alex.flink.beans;

import java.sql.Timestamp;

/**
 * @author liangxiaofei
 * @date 2021/4/28 14:15
 */
public class DataRoom {
    private Integer id;
    private Integer collector_id;
    private String collector_number;
    private Integer enterprise_id;
    private Timestamp data_time;
    private Float temperature;
    private Float humidity;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getCollector_id() {
        return collector_id;
    }

    public void setCollector_id(Integer collector_id) {
        this.collector_id = collector_id;
    }

    public String getCollector_number() {
        return collector_number;
    }

    public void setCollector_number(String collector_number) {
        this.collector_number = collector_number;
    }

    public Integer getEnterprise_id() {
        return enterprise_id;
    }

    public void setEnterprise_id(Integer enterprise_id) {
        this.enterprise_id = enterprise_id;
    }

    public Timestamp getData_time() {
        return data_time;
    }

    public void setData_time(Timestamp data_time) {
        this.data_time = data_time;
    }

    public Float getTemperature() {
        return temperature;
    }

    public void setTemperature(Float temperature) {
        this.temperature = temperature;
    }

    public Float getHumidity() {
        return humidity;
    }

    public void setHumidity(Float humidity) {
        this.humidity = humidity;
    }

    @Override
    public String toString() {
        return "DataRoom{" +
                "id=" + id +
                ", collector_id=" + collector_id +
                ", collector_number='" + collector_number + '\'' +
                ", enterprise_id=" + enterprise_id +
                ", data_time=" + data_time +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                '}';
    }
}
