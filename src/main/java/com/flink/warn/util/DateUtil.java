package com.flink.warn.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Author : chenhao
 * @Date : 2020/9/4 0004 11:04
 */
public class DateUtil {


    public  static String formatTime(long time, DateTimeFormatter formatter){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
        return localDateTime.format(formatter) + "+0800";
    }

}
