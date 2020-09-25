package com.flink.warn.entiy;

/**
 * @Author : chenhao
 * @Date : 2020/8/6 0006 19:05
 */
public class OriginalEvent {

    /**
     * 防火墙、漏扫等
     */
    private String deviceType;

    /**
     * 设备型号
     */
    private String deviceMode;

    /**
     * 安全事件类型
     */
    private String eventType;

    /**
     * 安全事件子类型
     */
    private String eventSubType;

    private String logType;

    private String attackType;

    /**
     * 事件名称
     */
    private String eventName;

    private String userName;

    private String protocol;

    private String deviceIp;

    private String srcCountry;

    private String srcCity;
    
    private String srcIp;

    private Integer srcPort;

    private String srcMac;

    private String dstIp;

    private Integer dstPort;

    private String dstMac;

    private Long up;

    private Long down;

    private Long startTime;

    private Long endTime;

    /**
     * 接收时间
     */
    private Long receiveTime;

    /***********************保留字段******************************/

    private String userdata1;

    private String userdata2;

    private String userdata3;

    private String userdata4;

    private String userdata5;

    private String userdata6;

    private String userdata7;

    private String userdata8;

    private String userdata9;

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

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventSubType() {
        return eventSubType;
    }

    public void setEventSubType(String eventSubType) {
        this.eventSubType = eventSubType;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getAttackType() {
        return attackType;
    }

    public void setAttackType(String attackType) {
        this.attackType = attackType;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getDeviceIp() {
        return deviceIp;
    }

    public void setDeviceIp(String deviceIp) {
        this.deviceIp = deviceIp;
    }

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

    public Integer getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(Integer srcPort) {
        this.srcPort = srcPort;
    }

    public String getSrcMac() {
        return srcMac;
    }

    public void setSrcMac(String srcMac) {
        this.srcMac = srcMac;
    }

    public String getDstIp() {
        return dstIp;
    }

    public void setDstIp(String dstIp) {
        this.dstIp = dstIp;
    }

    public Integer getDstPort() {
        return dstPort;
    }

    public void setDstPort(Integer dstPort) {
        this.dstPort = dstPort;
    }

    public String getDstMac() {
        return dstMac;
    }

    public void setDstMac(String dstMac) {
        this.dstMac = dstMac;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Long getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(Long receiveTime) {
        this.receiveTime = receiveTime;
    }

    public String getUserdata1() {
        return userdata1;
    }

    public void setUserdata1(String userdata1) {
        this.userdata1 = userdata1;
    }

    public String getUserdata2() {
        return userdata2;
    }

    public void setUserdata2(String userdata2) {
        this.userdata2 = userdata2;
    }

    public String getUserdata3() {
        return userdata3;
    }

    public void setUserdata3(String userdata3) {
        this.userdata3 = userdata3;
    }

    public String getUserdata4() {
        return userdata4;
    }

    public void setUserdata4(String userdata4) {
        this.userdata4 = userdata4;
    }

    public String getUserdata5() {
        return userdata5;
    }

    public void setUserdata5(String userdata5) {
        this.userdata5 = userdata5;
    }

    public String getUserdata6() {
        return userdata6;
    }

    public void setUserdata6(String userdata6) {
        this.userdata6 = userdata6;
    }

    public String getUserdata7() {
        return userdata7;
    }

    public void setUserdata7(String userdata7) {
        this.userdata7 = userdata7;
    }

    public String getUserdata8() {
        return userdata8;
    }

    public void setUserdata8(String userdata8) {
        this.userdata8 = userdata8;
    }

    public String getUserdata9() {
        return userdata9;
    }

    public void setUserdata9(String userdata9) {
        this.userdata9 = userdata9;
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
}
