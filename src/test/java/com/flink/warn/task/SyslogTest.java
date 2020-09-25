package com.flink.warn.task;

import com.alibaba.fastjson.JSONObject;
import org.graylog2.syslog4j.Syslog;
import org.graylog2.syslog4j.SyslogConstants;
import org.graylog2.syslog4j.SyslogIF;
import org.graylog2.syslog4j.impl.pool.generic.GenericSyslogPoolFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;

/**
 * @Author : chenhao
 * @Date : 2020/9/7 0007 11:45
 */
public class SyslogTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 32376;

    public void generate() {
        SyslogIF syslog = Syslog.getInstance(SyslogConstants.UDP);
        syslog.getConfig().setHost(HOST);
        syslog.getConfig().setPort(PORT);

        StringBuffer buffer = new StringBuffer();
        buffer.append("约会时间:" + new Date().toString().substring(4,20) + ";")
                .append("羞答答的美女:" + "我是小红啦" + ";")
                .append("暗号:" + "万般皆下品，唯有编码屌" + ";");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("message", buffer.toString());
        try {
            syslog.log(0, URLDecoder.decode(jsonObject.toString(), "utf-8"));
            syslog.shutdown();
        } catch (UnsupportedEncodingException e) {
            System.out.println("generate log get exception " + e);
        }
        System.out.println("哎呀，老娘的第一次dating，竟然还得先搭讪!");
    }

    public void pool(){
        GenericSyslogPoolFactory factory = new GenericSyslogPoolFactory();
        factory.createPool();
    }

    public static void main(String[] args) {
        new SyslogTest().generate();
    }
}
