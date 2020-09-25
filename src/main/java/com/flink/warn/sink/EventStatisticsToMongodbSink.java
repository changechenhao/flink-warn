package com.flink.warn.sink;

import com.flink.warn.MongoDBClient;
import com.flink.warn.entiy.EventStatistics;
import com.mongodb.DB;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author : chenhao
 * @Date : 2020/8/17 0017 20:14
 */
public class EventStatisticsToMongodbSink  extends RichSinkFunction<EventStatistics> {

    private static final String EVENT_STATISTICS = "event_statistics";

    private MongoCollection collection;

    private MongoDBClient mongoDBClient;

    private static final Logger logger = LoggerFactory.getLogger(EventStatisticsToMongodbSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("开始初始化EventStatisticsToMongodbSink.......");
        super.open(parameters);
        mongoDBClient = MongoDBClient.getInstance();
        if(mongoDBClient.getMongoClient() == null){
            mongoDBClient.init();
        }
        DB dataDb = mongoDBClient.getDataDb();
        Jongo jongo = new Jongo(dataDb);
        this.collection = jongo.getCollection(EVENT_STATISTICS);
        logger.info("初始化EventStatisticsToMongodbSink成功");
    }

    @Override
    public void close() throws Exception {
        super.close();
        mongoDBClient.close();
    }

    @Override
    public void invoke(EventStatistics value, SinkFunction.Context context) throws Exception {
        collection.insert(value);
    }
}
