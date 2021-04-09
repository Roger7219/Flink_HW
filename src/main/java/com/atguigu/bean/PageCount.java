package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/9 16:46
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageCount {
    private String url;
    private Long count;
    private Long windowEnd;
}
