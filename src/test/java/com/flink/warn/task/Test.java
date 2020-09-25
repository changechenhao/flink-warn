package com.flink.warn.task;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author : chenhao
 * @Date : 2020/9/7 0007 10:55
 */
public class Test {

    public static void main(String[] args) {
        Pattern p = Pattern.compile("([\\S\\s]+);\\s([\\S\\s]+):\\svirus_name=([\\S\\s]+);file_name=([\\S\\s]+);user_name=([\\s\\S]+);user_id=([\\s\\S]+);policy_id=([\\s\\S]+);src_mac=([\\s\\S]+);dst_mac=([\\s\\S]+);src_ip=([\\s\\S]+);dst_ip=([\\s\\S]+);src_port=([\\s\\S]+);dst_port=([\\s\\S]+);app_name=([\\s\\S]+);protocol=([\\s\\S]+);app_protocol=([\\s\\S]+);level=([\\s\\S]+);ctime=([\\s\\S]+);action=([\\S]+)");

        String content = "<4>Sep 18 16:59:59 ABT;190012000649827753386182;ipv4;3; av: virus_name=eicar_c;file_name=1.zip;user_name=172.16.16.100;user_id=2;policy_id=1;src_mac=00:1a:4a:16:01:91;dst_mac=00:1a:4a:16:01:95;src_ip=172.16.16.100;dst_ip=172.16.17.100;src_port=34931;dst_port=80;app_name=HTTP文件下载;protocol=TCP;app_protocol=HTTP;level=warning;ctime=2019-09-18 16:59:59;action=block";
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Matcher matcher = p.matcher(content);
            System.out.println(i + " - " + matcher.find());

        }

        System.out.println(System.currentTimeMillis() - start);
    }
}
