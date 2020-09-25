package com.flink.warn.entiy;

import org.jongo.marshall.jackson.oid.MongoId;
import org.jongo.marshall.jackson.oid.MongoObjectId;

/**
 * @Author : chenhao
 * @Date : 2020/9/17 0017 11:40
 */
public class WorkList {

    @MongoObjectId
    @MongoId
    private String id;

    private String mark;

    private String warnName;

    private String warnType;

    private Integer level;

    /**
     * 0-未处理，1-已处理，2-忽略
     */
    private Integer status;

    private Long createTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMark() {
        return mark;
    }

    public void setMark(String mark) {
        this.mark = mark;
    }

    public String getWarnName() {
        return warnName;
    }

    public void setWarnName(String warnName) {
        this.warnName = warnName;
    }

    public String getWarnType() {
        return warnType;
    }

    public void setWarnType(String warnType) {
        this.warnType = warnType;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "WorkList{" +
                "id='" + id + '\'' +
                ", warnName='" + warnName + '\'' +
                ", warnType='" + warnType + '\'' +
                ", level=" + level +
                ", status=" + status +
                ", createTime=" + createTime +
                '}';
    }
}
