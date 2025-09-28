package com.dataplatform.datadeduplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EntityScan(basePackages = "com.dataplatform.datadeduplication.model")
@EnableJpaRepositories(basePackages = "com.dataplatform.datadeduplication.repository")
@ComponentScan(basePackages = "com.dataplatform.datadeduplication")
public class DataDeduplicationApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataDeduplicationApplication.class, args);
    }
}