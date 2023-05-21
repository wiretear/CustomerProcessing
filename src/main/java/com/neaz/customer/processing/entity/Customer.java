package com.neaz.customer.processing.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name = "CUSTOMERS_INFO")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private long id;
    @Column(name = "first_name")
    private String firstName;
    @Column(name = "last_name")
    private String lastName;
    @Column(name = "state")
    private String state;
    @Column(name = "street")
    private String  street;
    @Column(name = "zip_code")
    private String  zipCode;
    @Column(name = "email")
    private String  email;
    @Column(name = "phone_number")
    private String  phoneNumber;
    @Column(name = "ip")
    private String  ip;
}
