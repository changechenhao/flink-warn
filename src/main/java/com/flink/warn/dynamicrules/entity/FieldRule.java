package com.flink.warn.dynamicrules.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.ToString;

/**
 * @Author : chenhao
 * @Date : 2020/11/23 0023 19:32
 */
@ToString
@Data
public class FieldRule {

    private String fieldName;

    private OperatorType operatorType;

    private String value;

    public boolean apply(String fieldValue) {
        switch (operatorType) {
            case EQUAL_STR:
                return fieldValue.equals(value);
            case NOT_EQUAL_STR:
                return !fieldValue.equals(value);
            case CONTAIN_STR:
                return fieldValue.contains(value);
            case NOT_CONTAIN_STR:
                return !fieldValue.contains(value);
            default:
                throw new RuntimeException("Unknown =operator type: " + operatorType);
        }
    }

    public boolean apply(Long fieldValue) {
        long lValue = Long.parseLong(value);
        switch (operatorType) {
            case EQUAL:
                return fieldValue.compareTo(lValue) == 0;
            case NOT_EQUAL:
                return fieldValue.compareTo(lValue) != 0;
            case GREATER:
                return fieldValue.compareTo(lValue) > 0;
            case LESS:
                return fieldValue.compareTo(lValue) < 0;
            case LESS_EQUAL:
                return fieldValue.compareTo(lValue) <= 0;
            case GREATER_EQUAL:
                return fieldValue.compareTo(lValue) >= 0;
            default:
                throw new RuntimeException("Unknown operator type: " + operatorType);
        }
    }

    public boolean apply(JSONObject object) {
        switch (operatorType.getDataType()) {
            case STRING:
                Long aLong = object.getLong(fieldName);
                return apply(aLong);
            case NUMBER:
                String string = object.getString(fieldName);
                return apply(string);
            default:
                throw new RuntimeException("Unknown data type: " + operatorType.getDataType());
        }
    }


    public enum OperatorType{

        /**
         * 字符串
         */
        EQUAL_STR("equal_str", DataType.STRING),
        NOT_EQUAL_STR("not_equal_str", DataType.STRING),
        CONTAIN_STR("contain_str", DataType.STRING),
        NOT_CONTAIN_STR("not_contain_str", DataType.STRING),

        /**
         * 数字
         */
        EQUAL("=", DataType.NUMBER),
        NOT_EQUAL("!=", DataType.NUMBER),
        GREATER_EQUAL(">=", DataType.NUMBER),
        LESS_EQUAL("<=", DataType.NUMBER),
        GREATER(">", DataType.NUMBER),
        LESS("<", DataType.NUMBER)
        ;

        private String type;
        private DataType dataType;

        OperatorType(String type, DataType dataType) {
            this.type = type;
            this.dataType = dataType;
        }

        public String getType() {
            return type;
        }

        public DataType getDataType() {
            return dataType;
        }
    }

    public enum DataType{

        STRING("String"),
        NUMBER("Number")
        ;


        private String type;

        DataType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

    }

}
