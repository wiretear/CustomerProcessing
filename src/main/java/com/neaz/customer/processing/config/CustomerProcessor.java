package com.neaz.customer.processing.config;

import com.neaz.customer.processing.entity.Customer;
import com.neaz.customer.processing.entity.InvalidCustomer;
import com.neaz.customer.processing.repository.InvalidCustomerRepository;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.RegexValidator;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {
    private static final RegexValidator PHONE_NUMBER_VALIDATOR = new RegexValidator("^(\\d{3}[-\\s.]?\\d{3}[-\\s.]?\\d{4}|1\\s\\d{3}\\s\\d{3}\\s\\d{4})$");
    private static final EmailValidator EMAIL_VALIDATOR = EmailValidator.getInstance();
    @Autowired
    private InvalidCustomerRepository invalidCustomerRepository;
    private FileWriter validDataWriter;
    private FileWriter invalidDataWriter;

    private Set<String> processedEmails = new HashSet<>();
    private Set<String> processedPhoneNumbers = new HashSet<>();

    public CustomerProcessor(FileWriter validDataWriter, FileWriter invalidDataWriter) {
        this.validDataWriter = validDataWriter;
        this.invalidDataWriter = invalidDataWriter;
    }

    @Override
    public Customer process(Customer customer) throws Exception {
        if (isValidCustomer(customer)) {
            if (isDuplicateData(customer)) {
                handleDuplicateData(customer);
                return null;
            } else {
                markDataAsProcessed(customer);
                writeValidDataToFile(customer);
                return customer;
            }
        } else {
            saveInvalidCustomer(customer);
            writeInvalidDataToFile(customer);
            return null;
        }
    }

    private boolean isValidCustomer(Customer customer) {
        String phoneNumber = customer.getPhoneNumber();
        String email = customer.getEmail();

        return isValidPhoneNumber(phoneNumber) && isValidEmail(email);
    }

    private boolean isValidPhoneNumber(String phoneNumber) {
        return PHONE_NUMBER_VALIDATOR.isValid(phoneNumber);
    }

    private boolean isValidEmail(String email) {
        return EMAIL_VALIDATOR.isValid(email);
    }

    private boolean isDuplicateData(Customer customer) {
        String phoneNumber = customer.getPhoneNumber();
        String email = customer.getEmail();

        return processedPhoneNumbers.contains(phoneNumber) || processedEmails.contains(email);
    }

    private void handleDuplicateData(Customer customer) {
        System.out.println("Duplicate customer data found: " + customer);
    }

    private void markDataAsProcessed(Customer customer) {
        processedPhoneNumbers.add(customer.getPhoneNumber());
        processedEmails.add(customer.getEmail());
    }

    private void writeValidDataToFile(Customer customer) throws IOException {
        validDataWriter.write(customer.toString() + "\n");
    }

    private void saveInvalidCustomer(Customer customer) {
        InvalidCustomer invalidCustomer = new InvalidCustomer();
        invalidCustomer.setFirstName(customer.getFirstName());
        invalidCustomer.setLastName(customer.getLastName());
        invalidCustomer.setState(customer.getState());
        invalidCustomer.setStreet(customer.getStreet());
        invalidCustomer.setZipCode(customer.getZipCode());
        invalidCustomer.setPhoneNumber(customer.getPhoneNumber());
        invalidCustomer.setEmail(customer.getEmail());
        invalidCustomer.setIp(customer.getIp());
        invalidCustomerRepository.save(invalidCustomer);
    }

    private void writeInvalidDataToFile(Customer customer) throws IOException {
        invalidDataWriter.write(customer.toString() + "\n");
    }
}