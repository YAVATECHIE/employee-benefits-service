package com.example.employeebenefits.repository;

import com.example.employeebenefits.domain.BenefitRequest;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BenefitRequestRepository extends JpaRepository<BenefitRequest, Long> {
}
