package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/9 16:18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotItem {
    private Long itemId;
    private Long count;
    private Long windowEndTime;
}

