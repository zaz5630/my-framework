package cn.azzhu.flink.test.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 测试数据，json数据格式中包含json对象
 * @author azzhu
 * @since 2021-02-18 23:29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestRecord {
    private Integer num;
    private Long ts;
    private String vin;
    private StatusKeyValueMap statusKeyValueMap;
}


