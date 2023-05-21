package com.neaz.customer.processing.repository;

import com.neaz.customer.processing.entity.InvalidCustomer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InvalidCustomerRepository extends JpaRepository<InvalidCustomer,Long> {
}
