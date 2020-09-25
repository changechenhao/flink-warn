package com.flink.warn.task;

import com.alibaba.fastjson.JSONObject;
import com.flink.warn.CustomEventTimeTrigger;
import com.flink.warn.CustomWarnWindowAssigner;
import com.flink.warn.FlowSumAggregateFunction;
import com.flink.warn.WarnCountAggregateFunction;
import com.flink.warn.config.PropertiesConfig;
import com.flink.warn.config.WarnRuleConfig;
import com.flink.warn.entiy.*;
import com.flink.warn.sink.WorkListToMongodbSink;
import com.flink.warn.util.DateUtil;
import com.flink.warn.util.ElasticSearchSinkUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import static com.flink.warn.constants.PropertiesConstants.*;

/**
 * @Author : chenhao
 * @Date : 2020/8/17 0017 11:04
 */
public class StatisticsTask {

    private static final OutputTag<OriginalEvent> outputWarnTag = new OutputTag<>("side-output-warn", TypeInformation.of(OriginalEvent.class));

    private static final OutputTag<OriginalEvent> outputFlowTag = new OutputTag<>("side-output-flow", TypeInformation.of(OriginalEvent.class));

    private static final OutputTag<Warn> workListFlowTag = new OutputTag<>("side-output-flow", TypeInformation.of(Warn.class));

    public static final String STATISTIC_TRAFFIC = "statistic_traffic";

    private static ElasticsearchConfig elasticsearchConfig;

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private static final Logger logger = LoggerFactory.getLogger(StatisticsTask.class);

    public static void main(String[] args) throws Exception {
        //设置配置文件路径
        String filePath = "/home/task.properties";
        String esFilePath = "/home/es.properties";
//        String filePath = "D:\\code\\work\\CSMReport\\ossimwarn\\src\\main\\resources\\task.properties";
//        String esFilePath = "D:\\code\\work\\CSMReport\\ossimwarn\\src\\main\\resources\\es.properties";
        if(Objects.nonNull(args) && args.length > 0){
            filePath = args[0];
        }
        PropertiesConfig config = new PropertiesConfig(filePath);

        PropertiesConfig esConfig = new PropertiesConfig(esFilePath);

        elasticsearchConfig = ElasticsearchConfig.create(esConfig);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        Properties props = buildKafkaProperties(config);

        long maxWatermarkLag = config.getLongValue(FLINK_MAXWATERMARKLAG);

        int slid = config.getIntValue(FLINK_WARN_SLID);

        SingleOutputStreamOperator<OriginalEvent> mainDataStream  = getMainDataStream(config, env, props, maxWatermarkLag);
        //统计任务
        statisticsTask(mainDataStream);
        //告警任务
        warnTask(slid, mainDataStream);
        //流量任务
        flowTask(mainDataStream);

        env.execute("flink task start.....");
    }

    private static void warnTask(int slid, SingleOutputStreamOperator<OriginalEvent> mainDataStream) {
        DataStream<OriginalEvent> sideOutput = mainDataStream.getSideOutput(outputWarnTag);
        SingleOutputStreamOperator<Warn> streamOperator = sideOutput.keyBy("srcIp", "deviceIp", "logType", "dstIp", "dstMac", "protocol", "srcPort", "dstPort")
                .window(new CustomWarnWindowAssigner(slid))
                .trigger(CustomEventTimeTrigger.create())
                .aggregate(new WarnCountAggregateFunction(), new ProcessWindowFunction<Tuple2<Long, Long>, Warn, Tuple, TimeWindow>() {

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Long, Long>> elements, Collector<Warn> out) throws Exception {

                        String logType = tuple.getField(2).toString();
                        WarnRule warnRule = WarnRuleConfig.getWarnRule(logType);

                        Tuple2<Long, Long> next = elements.iterator().next();
                        if (warnRule.getCount() > next.f1) {
                            return;
                        }
                        Warn warn = new Warn();
                        warn.setOccurTime(DateUtil.formatTime(next.f0, formatter));
                        warn.setStartTime(DateUtil.formatTime(context.window().getStart(), formatter));
                        warn.setEndTime(DateUtil.formatTime(context.window().getEnd(), formatter));
                        warn.setCreateTime(DateUtil.formatTime(System.currentTimeMillis(), formatter));
                        if(Objects.nonNull(tuple.getField(0))){
                            warn.setSrcIp(tuple.getField(0).toString());
                        }
                        if(Objects.nonNull(tuple.getField(1))){
                            warn.setDeviceIp(tuple.getField(1).toString());
                        }
                        if(Objects.nonNull(tuple.getField(2))){
                            warn.setLogType(tuple.getField(2).toString());
                        }
                        if(Objects.nonNull(tuple.getField(3))){
                            warn.setDstIp(tuple.getField(3).toString());
                        }
                        if(Objects.nonNull(tuple.getField(4))){
                            warn.setDstMac(tuple.getField(4).toString());
                        }
                        if(Objects.nonNull(tuple.getField(5))){
                            warn.setProtocol(tuple.getField(5).toString());
                        }
                        if(Objects.nonNull(tuple.getField(6))){
                            warn.setSrcPort(Integer.parseInt(tuple.getField(6).toString()));
                        }
                        if(Objects.nonNull(tuple.getField(7))){
                            warn.setDstPort(Integer.parseInt(tuple.getField(7).toString()));
                        }
                        warn.setLevel(warnRule.getLevel());
                        warn.setHandleStatus(0);
                        warn.setWarnRuleId(warnRule.getId());
                        warn.setWarnType(warnRule.getWarnType());
                        warn.setWarnName(warnRule.getWarnName());
                        warn.setCount(next.f1);
                        warn.setMark(UUID.randomUUID().toString());
                        System.out.println(warn.toString());

                        context.output(workListFlowTag, warn);
                        out.collect(warn);
                    }
                }).name("warn-task");

        ElasticSearchSinkUtil.addSink(elasticsearchConfig, "es-warn",streamOperator,
                (Warn warn, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index("warn")  //es 索引名
                            .source(JSONObject.toJSONBytes(warn), XContentType.JSON));
                });


        streamOperator.getSideOutput(workListFlowTag)
                .addSink(new WorkListToMongodbSink())
                .name("work_list");


    }

    private static void statisticsTask(SingleOutputStreamOperator<OriginalEvent> mainDataStream) {
        SingleOutputStreamOperator<EventStatistics> streamOperator = mainDataStream.keyBy("srcIp", "deviceIp", "logType", "dstIp", "srcPort", "dstPort", "eventType", "attackType", "deviceType", "deviceMode", "srcCountry", "srcCity", "eventName")
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(1)))
                .trigger(CustomEventTimeTrigger.create())
                .aggregate(new CountAggregateFunction(), new ProcessWindowFunction<Long, EventStatistics, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<EventStatistics> out) throws Exception {

                        if (tuple.getField(2) == null) {
                            return;
                        }
                        Long sum = elements.iterator().next();
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        EventStatistics statistics = new EventStatistics();
                        if(Objects.nonNull(tuple.getField(0))){
                            statistics.setSrcIp(tuple.getField(0).toString());
                        }
                        if(Objects.nonNull(tuple.getField(1))){
                            statistics.setDeviceIp(tuple.getField(1).toString());
                        }
                        if(Objects.nonNull(tuple.getField(2))){
                            statistics.setLogType(tuple.getField(2).toString());
                        }
                        if(Objects.nonNull(tuple.getField(3))){
                            statistics.setDstIp(tuple.getField(3).toString());
                        }
                        if(Objects.nonNull(tuple.getField(4))){
                            statistics.setSrcPort(Integer.parseInt(tuple.getField(4).toString()));
                        }
                        if(Objects.nonNull(tuple.getField(5))){
                            statistics.setDstPort(Integer.parseInt(tuple.getField(5).toString()));
                        }
                        if(Objects.nonNull(tuple.getField(6))){
                            statistics.setEventType(tuple.getField(6).toString());
                        }
                        if(Objects.nonNull(tuple.getField(7))){
                            statistics.setAttackType(tuple.getField(7).toString());
                        }
                        if(Objects.nonNull(tuple.getField(8))){
                            statistics.setDeviceType(tuple.getField(8).toString());
                        }
                        if(Objects.nonNull(tuple.getField(9))){
                            statistics.setDeviceMode(tuple.getField(9).toString());
                        }
                        if(Objects.nonNull(tuple.getField(10))){
                            statistics.setSrcCountry(tuple.getField(10).toString());
                        }
                        if(Objects.nonNull(tuple.getField(11))){
                            statistics.setSrcCity(tuple.getField(11).toString());
                        }
                        if(Objects.nonNull(tuple.getField(12))){
                            statistics.setEventName(tuple.getField(12).toString());
                        }
                        statistics.setCount(sum);
                        statistics.setStartTime(DateUtil.formatTime(start, formatter));
                        statistics.setEndTime(DateUtil.formatTime(end, formatter));
                        statistics.setCreateTime(DateUtil.formatTime(System.currentTimeMillis(), formatter));
                        System.out.println(statistics.toString());
                        out.collect(statistics);
                    }

                }).name("statistics-task");


        ElasticSearchSinkUtil.addSink(elasticsearchConfig, "es-statistics", streamOperator,
                (EventStatistics eventStatistics, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index("event_statistics")  //es 索引名
                            .source(JSONObject.toJSONBytes(eventStatistics), XContentType.JSON));
                });
    }

    private static void flowTask(SingleOutputStreamOperator<OriginalEvent> mainDataStream) {
        DataStream<OriginalEvent> sideOutput = mainDataStream.getSideOutput(outputFlowTag);
        SingleOutputStreamOperator<FwFlow> streamOperator = sideOutput
                .filter(new FilterFunction<OriginalEvent>() {
                    @Override
                    public boolean filter(OriginalEvent value) throws Exception {
                        return Objects.nonNull(value.getUp()) || Objects.nonNull(value.getDown());
                    }
                }).name("flow_filter")
                .keyBy("srcIp", "deviceIp")
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(1)))
                .trigger(CustomEventTimeTrigger.create())
                .aggregate(new FlowSumAggregateFunction(), new ProcessWindowFunction<Tuple2<Long, Long>, FwFlow, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Long, Long>> elements, Collector<FwFlow> out) throws Exception {
                        FwFlow fwFlow = new FwFlow();
                        if (Objects.nonNull(tuple.getField(0))) {
                            fwFlow.setSrcIp(tuple.getField(0).toString());
                        } else {
                            return;
                        }
                        if (Objects.nonNull(tuple.getField(1))) {
                            fwFlow.setDeviceIp(tuple.getField(1).toString());
                        } else {
                            return;
                        }

                        Tuple2<Long, Long> next = elements.iterator().next();
                        if(Objects.nonNull(next)){
                            if(Objects.nonNull(next.f0)){
                               fwFlow.setUp(Long.parseLong(next.f0.toString()));
                            }
                        }
                        if(Objects.nonNull(next)){
                            if(Objects.nonNull(next.f1)){
                                fwFlow.setDown(Long.parseLong(next.f1.toString()));
                            }
                        }

                        TimeWindow window = context.window();
                        fwFlow.setStartTime(DateUtil.formatTime(window.getStart(), formatter));
                        fwFlow.setEndTime(DateUtil.formatTime(window.getEnd(), formatter));

                        System.out.println(fwFlow.toString());
                        out.collect(fwFlow);
                    }

                }).name("flow-task");

        ElasticSearchSinkUtil.addSink(elasticsearchConfig, "es-flow", streamOperator,
                (FwFlow fwFlow, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index("flow")  //es 索引名
                            .source(JSONObject.toJSONBytes(fwFlow), XContentType.JSON));
                });
    }

    private static SingleOutputStreamOperator<OriginalEvent> getMainDataStream(PropertiesConfig config, StreamExecutionEnvironment env, Properties props, long maxWatermarkLag) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env.addSource(new FlinkKafkaConsumer010<String>(config.getString(KAFKA, TOPTIC), new SimpleStringSchema(), props)).name("kafka-mytopic-task-group")
                .setParallelism(1)
                .map(string -> JSONObject.parseObject(string, OriginalEvent.class)).name("map-to-event")
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<OriginalEvent>() {

                    private long currentTimestamp = Long.MIN_VALUE;

                    private final long maxTimeLag = maxWatermarkLag;

                    @Override
                    public Watermark checkAndGetNextWatermark(OriginalEvent lastElement, long extractedTimestamp) {
                        return new Watermark(currentTimestamp - maxTimeLag);
                    }

                    @Override
                    public long extractTimestamp(OriginalEvent element, long previousElementTimestamp) {
                        long timestamp = element.getReceiveTime();
                        currentTimestamp = Math.max(timestamp, currentTimestamp);
                        return timestamp;
                    }

                })
                .process(new ProcessFunction<OriginalEvent, OriginalEvent>() {


                    @Override
                    public void processElement(OriginalEvent value, Context ctx, Collector<OriginalEvent> out) throws Exception {
                        if(STATISTIC_TRAFFIC.equals(value.getLogType())){
                            ctx.output(outputFlowTag, value);
                        }else{
                            out.collect(value);
                        }

                        if (WarnRuleConfig.getWarnRule(value.getLogType()) != null) {
                            ctx.output(outputWarnTag, value);
                        }
                    }

                }).name("side-output");
    }

    private static class CountAggregateFunction implements AggregateFunction<OriginalEvent, Long, Long> {


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

    private static Properties buildKafkaProperties(PropertiesConfig config) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, config.getString(KAFKA, BOOTSTRAP_SERVERS));
        props.put(ZOOKEEPER_CONNECT, config.getString(KAFKA, ZOOKEEPER_CONNECT));
        props.put(GROUP_ID, config.getString(KAFKA, GROUP_ID));
        props.put(KEY_DESERIALIZER, config.getString(KAFKA, KEY_DESERIALIZER));
        props.put(VALUE_DESERIALIZER, config.getString(KAFKA, VALUE_DESERIALIZER));
        props.put(AUTO_OFFSET_RESET, config.getString(KAFKA, AUTO_OFFSET_RESET));
        return props;
    }

}
