package com.dataplatform.datanormalization;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EntityScan(basePackages = "com.dataplatform.datanormalization.model")
@EnableJpaRepositories(basePackages = "com.dataplatform.datanormalization.repository")
@ComponentScan(basePackages = "com.dataplatform.datanormalization")
public class DataNormalizationApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataNormalizationApplication.class, args);
    }
}