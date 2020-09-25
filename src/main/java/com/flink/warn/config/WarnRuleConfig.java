package com.flink.warn.config;

import com.flink.warn.MongoDBClient;
import com.flink.warn.entiy.WarnRule;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mongodb.DB;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author : chenhao
 * @Date : 2020/8/13 0013 10:11
 */
public class WarnRuleConfig {

    private static LoadingCache<String, Map<String, WarnRule>> cache = CacheBuilder.newBuilder()
            .maximumSize(1)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .refreshAfterWrite(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Map<String, WarnRule>>() {
                @Override
                public Map<String, WarnRule> load(String key) throws Exception {
                    return loadCache();
                }
            });

    private static final String WARN_RULE = "warn_rule";

    private static MongoCollection mongoCollection;

    static {
        MongoDBClient mongoDBClient = MongoDBClient.getInstance();
        DB db = mongoDBClient.getDataDb();
        Jongo jongo = new Jongo(db);
        mongoCollection = jongo.getCollection(WARN_RULE);
    }



    private static Map<String, WarnRule> loadCache(){
        MongoCursor<WarnRule> as = mongoCollection.find("{enableStatus:1}").as(WarnRule.class);
        List<WarnRule> list = new ArrayList<>();
        Iterator<WarnRule> iterator = as.iterator();
        while (iterator.hasNext()){
            list.add(iterator.next());
        }
        Map<String, WarnRule> map = list.stream().collect(Collectors.toMap(WarnRule::getLogType, Function.identity()));
        return map == null ? new HashMap<>(2) : map;
    }

    public static Map<String, WarnRule> getConfigMap(){
        try {
            return cache.get(WARN_RULE);
        } catch (ExecutionException e) {
            return null;
        }
    }

    public static WarnRule getWarnRule(String type){
        return Optional.ofNullable(getConfigMap())
                .map(item -> item.get(type))
                .orElseGet(() -> null);
    }
}
