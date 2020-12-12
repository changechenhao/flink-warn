package com.flink.warn.sink;

import com.flink.warn.MongoDBClient;
import com.flink.warn.entiy.WorkList;
import com.mongodb.DB;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

/**
 * @Author : chenhao
 * @Date : 2020/9/17 0017 11:50
 */
public class WorkListToMongodbSink  extends RichSinkFunction<WorkList> {


    private MongoCollection collection;

    private MongoDBClient mongoDBClient;

    private static final String table = "work_list";

    @Override
    public void open(Configuration parameters) throws Exception {
        if(mongoDBClient == null){
            this.mongoDBClient = MongoDBClient.getInstance();
            this.mongoDBClient.init();
            DB dataDb = mongoDBClient.getDataDb();
            Jongo jongo = new Jongo(dataDb);
            this.collection = jongo.getCollection(table);
        }

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void invoke(WorkList value, Context context) throws Exception {
        collection.insert(value);
    }
}
