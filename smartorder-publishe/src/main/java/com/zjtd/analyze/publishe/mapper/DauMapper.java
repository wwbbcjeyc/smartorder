package com.zjtd.analyze.publishe.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    /**
     * 查询某日用户活跃总数
     */

    public Long selectDauTotal(String date);
    public List<Map> selectDauTotalHourMap(String date);



}
