package com.dataplatform.dataquality;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EntityScan(basePackages = "com.dataplatform.dataquality.model")
@EnableJpaRepositories(basePackages = "com.dataplatform.dataquality.repository")
@ComponentScan(basePackages = "com.dataplatform.dataquality")
public class DataQualityApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataQualityApplication.class, args);
    }
}