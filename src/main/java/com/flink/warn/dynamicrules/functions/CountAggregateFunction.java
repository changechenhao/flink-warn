package com.flink.warn.dynamicrules.functions;

import com.flink.warn.dynamicrules.entity.OriginalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author : chenhao
 * @Date : 2020/12/3 0003 11:50
 */
public class CountAggregateFunction implements AggregateFunction<OriginalEvent, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(OriginalEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}