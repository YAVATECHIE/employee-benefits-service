package com.example.employeebenefits;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan
@SpringBootApplication
public class EmployeeBenefitsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(EmployeeBenefitsServiceApplication.class, args);
    }
}
