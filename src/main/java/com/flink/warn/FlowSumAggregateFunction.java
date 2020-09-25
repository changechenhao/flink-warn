package com.flink.warn;

import com.flink.warn.entiy.OriginalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

/**
 * @Author : chenhao
 * @Date : 2020/9/21 0021 20:50
 */
public class FlowSumAggregateFunction implements AggregateFunction<OriginalEvent, Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public Tuple2 createAccumulator() {
        return new Tuple2(0L, 0L);
    }

    @Override
    public Tuple2 add(OriginalEvent value, Tuple2 accumulator) {
        if(Objects.nonNull(value.getUp())) {
            accumulator.f0 = (Long) accumulator.f0 + value.getUp();
        }else{
            accumulator.f0 = (Long) accumulator.f0 + 0;
        }

        if(Objects.nonNull(value.getDown())) {
            accumulator.f1 = (Long) accumulator.f1 + value.getUp();
        }else{
            accumulator.f1 = (Long) accumulator.f1 + 0;
        }
        return accumulator;
    }

    @Override
    public Tuple2 getResult(Tuple2 accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2 merge(Tuple2 a, Tuple2 b) {
        return new Tuple2((Long)a.f0 + (Long)b.f0, (Long)a.f1 + (Long)b.f1);
    }
}
