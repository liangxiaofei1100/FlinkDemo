package com.alex.flink.roomTemp;

import java.util.Date;

/**
 * @author liangxiaofei
 * @date 2021/4/8 8:58
 */
public class RoomTempData {
    /**
     * 设备编号：200400072
     */
    private String clientSn;
    /**
     * 温度
     */
    private Float temperature;
    /**
     * 湿度
     */
    private Float humidity;
    /**
     * 数据时间
     */
    private Date dataTime;

    public String getClientSn() {
        return clientSn;
    }

    public void setClientSn(String clientSn) {
        this.clientSn = clientSn;
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

    public Date getDataTime() {
        return dataTime;
    }

    public void setDataTime(Date dataTime) {
        this.dataTime = dataTime;
    }

    @Override
    public String toString() {
        return "RoomTempData{" +
                "clientSn='" + clientSn + '\'' +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", dataTime=" + dataTime +
                '}';
    }
}
