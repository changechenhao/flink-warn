package com.flink.warn.dynamicrules.functions;

import com.alibaba.fastjson.JSONObject;
import com.flink.warn.sources.BaseGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * @Author : chenhao
 * @Date : 2020/12/2 0002 10:30
 */
public class JSONObjectGenerator extends BaseGenerator<String> {

    private static final List<String> logTypes = new ArrayList<>();
    private static final List<String> eventNames = new ArrayList<>();

    static {
        logTypes.add("av");
        logTypes.add("ips");
        logTypes.add("apt");
        logTypes.add("web");

        eventNames.add("病毒攻击");
        eventNames.add("IPS攻击");
        eventNames.add("异常包攻击");
        eventNames.add("WEB攻击");

    }

    public JSONObjectGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    public String randomEvent(SplittableRandom rnd, long id) {
        JSONObject result = new JSONObject();
        result.put("srcIp", generatorIp(rnd));
        result.put("dstIp", generatorIp(rnd));
        result.put("dstPort", 1);
        result.put("srcPort", 2);
        result.put("deviceIp", "192.168.2.150");
        result.put("logType", logType(1));
        result.put("eventType", 1);
        result.put("eventSubType", 1);
        result.put("startTime", System.currentTimeMillis());
        result.put("eventName", eventName(1));
        result.put("tag", 1);

        result.put("startTime", System.currentTimeMillis());
        return result.toJSONString();
    }

    private String generatorIp(SplittableRandom rnd){
        return "192.168.2." + rnd.nextInt(3);
    }

    private String logType(long id){
        long index = id % logTypes.size();
        return logTypes.get((int) index);
    }

    private String eventName(long id){
        long index = id % eventNames.size();
        return logTypes.get((int) index);
    }
}
