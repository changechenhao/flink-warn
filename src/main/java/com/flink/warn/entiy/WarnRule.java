package com.flink.warn.entiy;

import org.jongo.marshall.jackson.oid.MongoId;
import org.jongo.marshall.jackson.oid.MongoObjectId;

/**
 * @Author : chenhao
 * @Date : 2020/8/13 0013 10:46
 */
public class WarnRule {

    @MongoId
    @MongoObjectId
    private String id;

    private String warnType;

    private String warnName;

    private String logType;

    private int count;

    private int time;

    private int level;

    private int enableStatus;

    public WarnRule() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getEnableStatus() {
        return enableStatus;
    }

    public void setEnableStatus(int enableStatus) {
        this.enableStatus = enableStatus;
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

    @Override
    public String toString() {
        return "WarnRule{" +
                "id='" + id + '\'' +
                ", warnType='" + warnType + '\'' +
                ", warnName='" + warnName + '\'' +
                ", logType='" + logType + '\'' +
                ", count=" + count +
                ", time=" + time +
                ", level=" + level +
                ", enableStatus=" + enableStatus +
                '}';
    }
}
