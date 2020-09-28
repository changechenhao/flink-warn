package com.flink.warn.config;

import com.flink.warn.MongoDBClient;
import com.flink.warn.entiy.WarnPolicy;
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
public class WarnPolicyConfig {

    private static LoadingCache<String, Map<String, WarnPolicy>> cache = CacheBuilder.newBuilder()
            .maximumSize(1)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .refreshAfterWrite(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Map<String, WarnPolicy>>() {
                @Override
                public Map<String, WarnPolicy> load(String key) throws Exception {
                    return loadCache();
                }
            });

    private static final String WARN_POLICY = "warn_policy";

    private static MongoCollection mongoCollection;

    static {
        MongoDBClient mongoDBClient = MongoDBClient.getInstance();
        DB db = mongoDBClient.getDataDb();
        Jongo jongo = new Jongo(db);
        mongoCollection = jongo.getCollection(WARN_POLICY);
    }



    private static Map<String, WarnPolicy> loadCache(){
        MongoCursor<WarnPolicy> as = mongoCollection.find("{enableStatus:1}").as(WarnPolicy.class);
        List<WarnPolicy> list = new ArrayList<>();
        Iterator<WarnPolicy> iterator = as.iterator();
        while (iterator.hasNext()){
            list.add(iterator.next());
        }
        Map<String, WarnPolicy> map = list.stream().collect(Collectors.toMap(WarnPolicy::getLogType, Function.identity()));
        return map == null ? new HashMap<>(2) : map;
    }

    public static Map<String, WarnPolicy> getConfigMap(){
        try {
            return cache.get(WARN_POLICY);
        } catch (ExecutionException e) {
            return null;
        }
    }

    public static WarnPolicy getWarnPolicy(String type){
        return Optional.ofNullable(getConfigMap())
                .map(item -> item.get(type))
                .orElseGet(() -> null);
    }
}
