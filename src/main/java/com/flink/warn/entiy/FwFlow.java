package com.flink.warn.entiy;

/**
 * @Author : chenhao
 * @Date : 2020/9/22 0022 10:08
 */
public class FwFlow {

    private String srcIp;
    private String deviceIp;
    private Long up;
    private Long down;
    private String startTime;
    private String endTime;


    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public String getDeviceIp() {
        return deviceIp;
    }

    public void setDeviceIp(String deviceIp) {
        this.deviceIp = deviceIp;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
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

    @Override
    public String toString() {
        return "FwFlow{" +
                "srcIp='" + srcIp + '\'' +
                ", deviceIp='" + deviceIp + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
