package com.flink.warn.entiy;

/**
 * @Author : chenhao
 * @Date : 2020/8/13 0013 20:33
 */
public class Warn {

    /**
     * 告警规则Id
     */
    private String warnRuleId;

    /**
     * 工单标识，用于联系工单
     */
    private String mark;

    /**
     * 告警类型
     */
    private String warnType;

    private String warnName;

    private String logType;

    private String protocol;

    /**
     * 设备ip
     */
    private String deviceIp;

    private String srcIp;

    private Integer srcPort;

    private String dstIp;

    private Integer dstPort;

    private String dstMac;

    /**
     * 处理状态
     */
    private Integer handleStatus;

    /**
     * 危险等级
     */
    private Integer level;

    private Long count;

    /**
     * 发生时间
     */
    private String occurTime;

    /**
     * 开始时间
     */
    private String startTime;

    /**
     * 结束时间
     */
    private String endTime;

    /**
     * 创建时间
     */
    private String createTime;


    public String getWarnRuleId() {
        return warnRuleId;
    }

    public void setWarnRuleId(String warnRuleId) {
        this.warnRuleId = warnRuleId;
    }

    public String getMark() {
        return mark;
    }

    public void setMark(String mark) {
        this.mark = mark;
    }

    public String getWarnType() {
        return warnType;
    }

    public void setWarnType(String warnType) {
        this.warnType = warnType;
    }

    public String getWarnName() {
        return warnName;
    }

    public void setWarnName(String warnName) {
        this.warnName = warnName;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getDeviceIp() {
        return deviceIp;
    }

    public void setDeviceIp(String deviceIp) {
        this.deviceIp = deviceIp;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public Integer getHandleStatus() {
        return handleStatus;
    }

    public void setHandleStatus(Integer handleStatus) {
        this.handleStatus = handleStatus;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getOccurTime() {
        return occurTime;
    }

    public void setOccurTime(String occurTime) {
        this.occurTime = occurTime;
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

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getDstIp() {
        return dstIp;
    }

    public void setDstIp(String dstIp) {
        this.dstIp = dstIp;
    }

    public String getDstMac() {
        return dstMac;
    }

    public void setDstMac(String dstMac) {
        this.dstMac = dstMac;
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

    @Override
    public String toString() {
        return "Warn{" +
                "warnRuleId='" + warnRuleId + '\'' +
                ", mark='" + mark + '\'' +
                ", warnType='" + warnType + '\'' +
                ", warnName='" + warnName + '\'' +
                ", logType='" + logType + '\'' +
                ", protocol='" + protocol + '\'' +
                ", deviceIp='" + deviceIp + '\'' +
                ", srcIp='" + srcIp + '\'' +
                ", srcPort=" + srcPort +
                ", dstIp='" + dstIp + '\'' +
                ", dstPort=" + dstPort +
                ", dstMac='" + dstMac + '\'' +
                ", handleStatus=" + handleStatus +
                ", level=" + level +
                ", count=" + count +
                ", occurTime='" + occurTime + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
