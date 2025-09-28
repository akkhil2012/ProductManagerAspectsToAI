package com.dataplatform.datastorage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EntityScan(basePackages = "com.dataplatform.datastorage.model")
@EnableJpaRepositories(basePackages = "com.dataplatform.datastorage.repository")
@ComponentScan(basePackages = "com.dataplatform.datastorage")
public class DataStorageApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataStorageApplication.class, args);
    }
}