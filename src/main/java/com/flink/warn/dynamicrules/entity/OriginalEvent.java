package com.flink.warn.dynamicrules.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : chenhao
 * @Date : 2020/8/6 0006 19:05
 */
@Data
@NoArgsConstructor
public class OriginalEvent {

    private String srcIp;

    private String dstIp;

    private Integer srcPort;

    private Integer dstPort;

    private String srcCountry;

    private String srcProvince;

    private String srcCity;

    private String dstCountry;

    private String dstProvince;

    private String dstCity;

    private String deviceIp;

    private String level;

    private String assetType;

    private String assetSubType;

    private String protocol;

    private String logType;

    private String eventType;

    private String eventSubType;

    private String eventName;

    private Long startTime;

    private Long endTime;

    private Long receiveTime;

}
