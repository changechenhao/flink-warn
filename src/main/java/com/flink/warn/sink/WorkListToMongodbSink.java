package com.flink.warn.sink;

import com.flink.warn.MongoDBClient;
import com.flink.warn.entiy.Warn;
import com.flink.warn.entiy.WorkList;
import com.mongodb.DB;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

/**
 * @Author : chenhao
 * @Date : 2020/9/17 0017 11:50
 */
public class WorkListToMongodbSink  extends RichSinkFunction<Warn> {

    public static final String WORK_LIST = "work_list";

    private MongoCollection collection;

    private MongoDBClient mongoDBClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoDBClient = MongoDBClient.getInstance();
        if(mongoDBClient.getMongoClient() == null){
            mongoDBClient.init();
        }
        DB dataDb = mongoDBClient.getDataDb();
        Jongo jongo = new Jongo(dataDb);
        this.collection = jongo.getCollection(WORK_LIST);
    }

    @Override
    public void close() throws Exception {
        super.close();
        MongoDBClient.getInstance().close();
    }

    @Override
    public void invoke(Warn value, SinkFunction.Context context) throws Exception {
        WorkList workList = new WorkList();
        workList.setMark(value.getMark());
        workList.setCreateTime(System.currentTimeMillis());
        workList.setWarnName(value.getWarnName());
        workList.setWarnType(value.getWarnType());
        workList.setLevel(value.getLevel());
        workList.setStatus(0);
        collection.insert(workList);
    }
}
