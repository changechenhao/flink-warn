package com.flink.warn.entiy;

import lombok.Data;
import lombok.ToString;
import org.jongo.marshall.jackson.oid.MongoId;
import org.jongo.marshall.jackson.oid.MongoObjectId;

/**
 * @Author : chenhao
 * @Date : 2020/9/17 0017 11:40
 */
@ToString
@Data
public class WorkList {

    @MongoObjectId
    @MongoId
    private String id;

    private String mark;

    private String warnName;

    private String warnType;

    private String level;

    /**
     * 0-未处理，1-已处理，2-忽略
     */
    private Integer status;

    private Long createTime;

}
