package com.flink.warn.dynamicrules.functions;

import com.flink.warn.MongoDBClient;
import com.flink.warn.sink.EventStatisticsToMongodbSink;
import com.mongodb.DB;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Iterator;

/**
 * @Author : chenhao
 * @Date : 2020/12/12 0012 14:37
 */
public class MongoDBStreamFunction<OUT> extends RichParallelSourceFunction<OUT> {

    private volatile boolean isRun;

    private volatile int lastUpdateMin = -1;

    private String table;

    private MongoCollection collection;

    private MongoDBClient mongoDBClient;

    private Class<OUT> clazz;

    private static final Logger logger = LoggerFactory.getLogger(EventStatisticsToMongodbSink.class);

    public MongoDBStreamFunction(String table, Class<OUT> clazz) {
        this.table = table;
        this.clazz = clazz;
        if(StringUtils.isBlank(table)){
            throw new IllegalArgumentException("无效的表名");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("开始初始化MongoDBStreamFunction，table = {}", this.table);
        super.open(parameters);
        mongoDBClient = MongoDBClient.getInstance();
        if(mongoDBClient.getMongoClient() == null){
            mongoDBClient.init();
        }
        DB dataDb = mongoDBClient.getDataDb();
        Jongo jongo = new Jongo(dataDb);
        this.collection = jongo.getCollection(this.table);
        isRun = true;
        logger.info("初始化MongoDBStreamFunction成功");
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        while(isRun){
            LocalDateTime date = LocalDateTime.now();
            int min = date.getMinute();
            if(min != lastUpdateMin){
                lastUpdateMin = min;
                MongoCursor<OUT> as = collection.find("{ruleState:'ACTIVE'}").as(clazz);
                Iterator<OUT> iterator = as.iterator();
                while (iterator.hasNext()){
                    ctx.collect(iterator.next());
                }
            }
            Thread.sleep(1000 * 15);
        }
    }

    @Override
    public void cancel() {
        isRun = false;
    }


}
