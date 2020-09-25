package com.flink.warn;

import com.flink.warn.entiy.OriginalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author : chenhao
 * @Date : 2020/8/17 0017 16:08
 */
public class WarnCountAggregateFunction implements AggregateFunction<OriginalEvent, Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public Tuple2 createAccumulator() {
        return new Tuple2(0L, 0L);
    }

    @Override
    public Tuple2 add(OriginalEvent value, Tuple2 accumulator) {
        if((Long)accumulator.f0 == 0L){
            accumulator.f0 = value.getReceiveTime();

        }
        accumulator.f1 = (Long)accumulator.f1 + 1;
        return accumulator;
    }

    @Override
    public Tuple2 getResult(Tuple2 accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2 merge(Tuple2 a, Tuple2 b) {
        return new Tuple2(a.f0, (Long)a.f1 + (Long)b.f1);
    }
}