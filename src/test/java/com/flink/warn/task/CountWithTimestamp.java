package com.flink.warn.task;

import java.util.LinkedList;

/**
 * @Author : chenhao
 * @Date : 2020/8/12 0012 9:48
 */
public class CountWithTimestamp {

    public String key;

    public LinkedList<Long> times;

    public long count;

    public long lastModified;
}
