package com.flink.warn.dynamicrules;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : chenhao
 * @Date : 2020/12/1 0001 17:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Info {
    private String deviceIp;
    private String srcIp;
    private String dstIp;
    private String dstPort;
    private String srcPort;
    private String logType;
    private String eventType;
    private String eventSubType;
    private Long startTime;
}
