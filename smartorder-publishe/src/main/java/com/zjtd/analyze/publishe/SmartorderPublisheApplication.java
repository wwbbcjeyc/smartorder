package com.zjtd.analyze.publishe;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.zjtd.analyze.publishe.mapper")
public class SmartorderPublisheApplication {

    public static void main(String[] args) {
        SpringApplication.run(SmartorderPublisheApplication.class, args);
    }

}
