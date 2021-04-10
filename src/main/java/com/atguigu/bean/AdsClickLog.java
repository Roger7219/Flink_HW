package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/3/9 9:49
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdsClickLog {
    private long userId;
    private long adsId;
    private String province;
    private String city;
    private Long timestamp;

}
