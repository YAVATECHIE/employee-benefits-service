package com.example.employeebenefits.repository;

import com.example.employeebenefits.domain.BenefitRequest;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface BenefitRequestRepository extends JpaRepository<BenefitRequest, Long> {
    Optional<BenefitRequest> findByRequestId(String requestId);
}
