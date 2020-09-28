package com.flink.warn.sink;

import com.flink.warn.MongoDBClient;
import com.flink.warn.entiy.Warn;
import com.mongodb.DB;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

/**
 * @Author : chenhao
 * @Date : 2020/8/17 0017 19:57
 */
public class WarnToMongodbSink  extends RichSinkFunction<Warn> {

    private static final String WARN = "warn";

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
        this.collection = jongo.getCollection(WARN);
    }

    @Override
    public void close() throws Exception {
        super.close();
        MongoDBClient.getInstance().close();
    }

    @Override
    public void invoke(Warn value, Context context) throws Exception {
        collection.insert(value);
    }
}
