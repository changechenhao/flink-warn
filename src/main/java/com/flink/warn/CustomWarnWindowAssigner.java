package com.flink.warn;

import com.flink.warn.config.WarnRuleConfig;
import com.flink.warn.entiy.OriginalEvent;
import com.flink.warn.entiy.WarnRule;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * @Author : chenhao
 * @Date : 2020/8/13 0013 9:40
 */
public class CustomWarnWindowAssigner extends WindowAssigner<OriginalEvent, TimeWindow> {

    private int slide;

    public CustomWarnWindowAssigner(int slide) {
        this.slide = slide;
    }

    @Override
    public Collection<TimeWindow> assignWindows(OriginalEvent element, long timestamp, WindowAssignerContext context) {
        WarnRule warnRule = WarnRuleConfig.getWarnRule(element.getLogType());
        List<TimeWindow> windows = new ArrayList<>();
        if(Objects.nonNull(warnRule)){
            int size = warnRule.getTime() * 1000;
            long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, 0, slide);
            for (long start = lastStart;
                 start > timestamp - size;
                 start -= slide) {
                windows.add(new TimeWindow(start, start + size));
            }
        }
        return windows;
    }

    @Override
    public Trigger<OriginalEvent, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        EventTimeTrigger eventTimeTrigger = EventTimeTrigger.create();
        return (Trigger) eventTimeTrigger;
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
