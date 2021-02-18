package cn.azzhu.flink.stream.util;

import lombok.Getter;

/**
 * Table 类型枚举类
 * @author azzhu
 * @since 2021-02-18 23:06
 */
@Getter
public enum TableTypeEnum {
    HIVE(1,"HIVE"),
    KAFKA(2,"KAFKA");

    public final Integer type;
    public final String value;

    TableTypeEnum(Integer type, String value) {
        this.type = type;
        this.value = value;
    }

}
