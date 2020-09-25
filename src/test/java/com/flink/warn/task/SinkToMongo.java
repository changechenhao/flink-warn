package com.flink.warn.task;

import com.flink.warn.entiy.EventStatistics;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Author : chenhao
 * @Date : 2020/8/17 0017 19:22
 */
public class SinkToMongo extends RichSinkFunction<EventStatistics> {
}
