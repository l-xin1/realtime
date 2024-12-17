package com.lx.reatlimepublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@MapperScan(basePackages = "com.lx.reatlimepublisher.mapper")
public class ReatlimePublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReatlimePublisherApplication.class, args);
    }
}
