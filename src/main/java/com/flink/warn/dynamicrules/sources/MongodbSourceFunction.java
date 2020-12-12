package com.flink.warn.dynamicrules.sources;

import com.flink.warn.MongoDBClient;
import com.flink.warn.dynamicrules.entity.WarnRule;
import com.mongodb.DB;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @Author : chenhao
 * @Date : 2020/12/12 0012 14:37
 */
public class MongodbSourceFunction implements SourceFunction<WarnRule> {

    private volatile boolean isRun;

    private volatile int lastUpdateMin = -1;

    private String table;

    private static final Logger logger = LoggerFactory.getLogger(MongodbSourceFunction.class);

    public MongodbSourceFunction(String table) {
        this.table = table;
        if(StringUtils.isBlank(table)){
            throw new IllegalArgumentException("无效的表名");
        }
        this.isRun = true;
    }

    @Override
    public void run(SourceContext<WarnRule> ctx) throws Exception {
        while(isRun){
            MongoDBClient instance = MongoDBClient.getInstance();
            DB dataDb = instance.getDataDb();
            Jongo jongo = new Jongo(dataDb);
            MongoCollection collection = jongo.getCollection(this.table);

            MongoCursor<WarnRule> as = collection.find("{enableStatus:1}").as(WarnRule.class);
            Iterator<WarnRule> iterator = as.iterator();
            while (iterator.hasNext()){
                ctx.collect(iterator.next());
            }
            Thread.sleep(1000 * 15);
        }
    }

    @Override
    public void cancel() {
    }


}
