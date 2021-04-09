package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/9 16:47
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLog {
    private String ip;
    private long eventTime;
    private String method;
    private String url;
}
