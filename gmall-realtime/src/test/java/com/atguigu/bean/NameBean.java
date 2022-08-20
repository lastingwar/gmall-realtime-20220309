package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2022-08-20 9:23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NameBean {
    String id;
    String name;
    Long ts;
}
