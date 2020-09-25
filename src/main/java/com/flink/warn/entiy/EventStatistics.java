package com.flink.warn.entiy;

/**
 * @Author : chenhao
 * @Date : 2020/8/17 0017 16:13
 */
public class EventStatistics {

    private String srcCountry;

    private String srcCity;

    private String srcIp;

    private String dstIp;

    private Integer srcPort;

    private Integer dstPort;

    private String deviceIp;

    private String deviceType;

    private String deviceMode;

    private String logType;

    private String eventType;

    private String eventName;

    private String attackType;

    private long count;

    private String startTime;

    private String endTime;

    private String createTime;

    public String getSrcCountry() {
        return srcCountry;
    }

    public void setSrcCountry(String srcCountry) {
        this.srcCountry = srcCountry;
    }

    public String getSrcCity() {
        return srcCity;
    }

    public void setSrcCity(String srcCity) {
        this.srcCity = srcCity;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public String getDstIp() {
        return dstIp;
    }

    public void setDstIp(String dstIp) {
        this.dstIp = dstIp;
    }

    public Integer getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(Integer srcPort) {
        this.srcPort = srcPort;
    }

    public Integer getDstPort() {
        return dstPort;
    }

    public void setDstPort(Integer dstPort) {
        this.dstPort = dstPort;
    }

    public String getDeviceIp() {
        return deviceIp;
    }

    public void setDeviceIp(String deviceIp) {
        this.deviceIp = deviceIp;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getDeviceMode() {
        return deviceMode;
    }

    public void setDeviceMode(String deviceMode) {
        this.deviceMode = deviceMode;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getAttackType() {
        return attackType;
    }

    public void setAttackType(String attackType) {
        this.attackType = attackType;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    @Override
    public String toString() {
        return "EventStatistics{" +
                "srcCountry='" + srcCountry + '\'' +
                ", srcCity='" + srcCity + '\'' +
                ", srcIp='" + srcIp + '\'' +
                ", dstIp='" + dstIp + '\'' +
                ", srcPort=" + srcPort +
                ", dstPort=" + dstPort +
                ", deviceIp='" + deviceIp + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", deviceMode='" + deviceMode + '\'' +
                ", logType='" + logType + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventName='" + eventName + '\'' +
                ", attackType='" + attackType + '\'' +
                ", count=" + count +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
