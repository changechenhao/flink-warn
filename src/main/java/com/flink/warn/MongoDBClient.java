package com.flink.warn;

import com.flink.warn.config.PropertiesConfig;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

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
        int threadsAllowed = 20;
        int socketTimeout = config.getIntValue(SOCKET_TIMEOUT);
        int maxConnectionIdleTime = config.getIntValue(MAX_CONNECTION_IDLET_IME);

        // 选项构建者
        MongoClientOptions options = new MongoClientOptions.Builder()
                .connectTimeout(connectTimeout)
                .maxConnectionIdleTime(maxConnectionIdleTime)
                .connectionsPerHost(connectionsPerHost)
                .threadsAllowedToBlockForConnectionMultiplier(threadsAllowed)
                .maxWaitTime(maxWaitTime)
                .socketTimeout(socketTimeout)
                .cursorFinalizerEnabled(false)
                .build();

        ServerAddress serverAddress = new ServerAddress(host, port);

        MongoCredential credential = MongoCredential.createCredential(username, logindb, password.toCharArray());

        mongoClient = new MongoClient(serverAddress, credential, options);
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
