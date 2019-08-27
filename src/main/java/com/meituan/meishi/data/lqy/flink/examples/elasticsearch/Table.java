package com.meituan.meishi.data.lqy.flink.examples.elasticsearch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liqingyong
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Table {

    private Integer id;

    private String tabName;

    private String tabChinese;

    private String owner;

    private String desc;

}
