package com.flink.warn.entiy;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : chenhao
 * @Date : 2020/8/17 0017 16:13
 */
@Data
@NoArgsConstructor
public class EventStatistics {

    private String srcIp;

    private String dstIp;

    private Integer srcPort;

    private Integer dstPort;

    private String srcCountry;

    private String srcProProvince;

    private String srcCity;

    private String dstCountry;

    private String dstProProvince;

    private String dstCity;

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


}
