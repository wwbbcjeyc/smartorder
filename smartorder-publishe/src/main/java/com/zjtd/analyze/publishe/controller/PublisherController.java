package com.zjtd.analyze.publishe.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.analyze.publishe.service.PublisherService;

import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dateString){
        // 日活总数
        Long dauTotal = publisherService.getDauTotal(dateString);

        List<Map> totalList =new ArrayList<>();
        HashMap dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);


        HashMap midMap = new HashMap();
        midMap.put("id","mid");
        midMap.put("name","新增设备");
        midMap.put("value",323);

        totalList.add(midMap);

        return JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hours")
    public String realtimeHourDate(@RequestParam("id") String id, @RequestParam("date") String date){

        if( "dau".equals(id)){
            Map dauHoursToday = publisherService.getDauHours(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",dauHoursToday);
            String  yesterdayDateString="";
            try {
                Date dateToday = new SimpleDateFormat("yyyy-MM-dd").parse(date);
                Date dateYesterday = DateUtils.addDays(dateToday, -1);
                yesterdayDateString=new SimpleDateFormat("yyyy-MM-dd").format(dateYesterday);

            } catch (ParseException e) {
                e.printStackTrace();
            }
            Map dauHoursYesterday = publisherService.getDauHours(yesterdayDateString);
            jsonObject.put("yesterday",dauHoursYesterday);
            return jsonObject.toJSONString();
        }


        /*if( "new_order_totalamount".equals(id)){
            String newOrderTotalamountJson = publisherService.getNewOrderTotalAmountHours(date);
            return newOrderTotalamountJson;
        }*/
        return null;
    }




}