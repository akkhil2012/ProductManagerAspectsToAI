package com.dataplatform.dataconsumption;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EntityScan(basePackages = "com.dataplatform.dataconsumption.model")
@EnableJpaRepositories(basePackages = "com.dataplatform.dataconsumption.repository")
@ComponentScan(basePackages = "com.dataplatform.dataconsumption")
public class DataConsumptionApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataConsumptionApplication.class, args);
    }
}