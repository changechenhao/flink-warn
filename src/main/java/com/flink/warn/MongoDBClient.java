package com.flink.warn;

import com.flink.warn.config.PropertiesConfig;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.flink.warn.constants.PropertiesConstants.*;

/**
 * @Author : chenhao
 * @Date : 2020/5/6 0006 10:28
 */
public class MongoDBClient {

    public static final String ADMIN = "admin";
    private static volatile MongoDBClient instance;

    private MongoClient mongoClient;

    private String db;

    private String logindb;

    private static final String confPath = "/home/mongodb.properties";
//    private static final String confPath = "D:\\code\\work\\CSMReport\\ossimwarn\\src\\main\\resources\\mongodb.properties";

    private MongoDBClient() {

    }

    public static MongoDBClient getInstance() {
        if (null == instance) {
            synchronized (MongoDBClient.class) {
                if (null == instance) {
                    instance = new MongoDBClient();
                    instance.init();
                }
            }
        }
        return instance;
    }

    public void init() {
        PropertiesConfig config = new PropertiesConfig(confPath);
        String host = config.getString(HOST);
        String username = config.getString(USERNAME);
        String password = config.getString(PASSWORD);
        db = config.getString(DB);
        logindb = config.getString(LOGIN_DB);
        int port = config.getIntValue(PORT);
        int connectTimeout = config.getIntValue(CONNECT_TIMEOUT);
        int maxWaitTime = config.getIntValue(MAX_WAIT_TIME);
        int connectionsPerHost = config.getIntValue(CONNECTIONS_PERHOST);
        int threadsAllowed = config.getIntValue(THREADS_ALLOWED);
        int socketTimeout = config.getIntValue(SOCKET_TIMEOUT);

        // 选项构建者
        com.mongodb.MongoClientOptions.Builder builder = new MongoClientOptions.Builder();

        // 设置连接超时时间
        builder.connectTimeout(connectTimeout);

        // 每个地址最大请求数
        builder.connectionsPerHost(connectionsPerHost);

        builder.threadsAllowedToBlockForConnectionMultiplier(threadsAllowed);

        // 设置最大等待时间
        builder.maxWaitTime(maxWaitTime);

        // 读取数据的超时时间
        builder.socketTimeout(socketTimeout);

        ServerAddress serverAddress = new ServerAddress(host, port);

        List<MongoCredential> credentials = new ArrayList<MongoCredential>();

        MongoCredential credential = MongoCredential.createCredential(username, logindb, password.toCharArray());

        credentials.add(credential);

        mongoClient=new MongoClient(serverAddress, Arrays.asList(credential));

    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public com.mongodb.DB getDataDb() {
        if(instance == null){
            getInstance();
        }

        if(mongoClient == null){
            init();
        }

        com.mongodb.DB mongodb = getMongoClient().getDB(this.db);
        return mongodb;
    }

    public void close(){
        MongoClient mongoClient = getMongoClient();
        if(Objects.nonNull(mongoClient)){
            mongoClient.close();
        }
    }

}
