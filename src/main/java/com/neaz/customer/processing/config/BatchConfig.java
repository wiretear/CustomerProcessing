package com.neaz.customer.processing.config;

import com.neaz.customer.processing.entity.Customer;
import com.neaz.customer.processing.partition.ColumnRangePartitioner;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.FileWriter;
import java.io.IOException;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class BatchConfig {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private CustomerWriter customerWriter;

    @Bean
    public FlatFileItemReader<Customer> reader() {
        FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("src/main/resources/1M-customers.txt"));
        reader.setLineMapper(new DefaultLineMapper<Customer>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("firstName", "lastName", "state", "street", "zipCode", "phoneNumber", "email", "ip");
                setDelimiter(",");
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Customer>() {{
                setTargetType(Customer.class);
            }});
        }});
        return reader;
    }

    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor(validDataWriter(), invalidDataWriter());
    }


    @Bean
    public FileWriter validDataWriter() {
        String filePath = "D:/File";
        try {
            return new FileWriter(filePath + "/valid-data.txt", true);
        } catch (IOException e) {

            throw new IllegalStateException("Failed to create valid-data.txt file", e);
        }
    }

    @Bean
    public FileWriter invalidDataWriter() {
        String filePath = "D:/File";
        try {
            return new FileWriter(filePath + "/invalid-data.txt", true);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create valid-data.txt file", e);
        }
    }

    @Bean
    public SkipPolicy incorrectTokenSkipPolicy() {
        return new SkipPolicy() {
            private static final int MAX_SKIP_COUNT = 10000000;

            @Override
            public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
                if (t instanceof FlatFileParseException) {
                    FlatFileParseException parseException = (FlatFileParseException) t;
                    String input = parseException.getInput();
                    int actualTokenCount = input.split(",").length;

                    if (actualTokenCount != 8) {
                        System.out.println("Incorrect number of tokens found in record: " + input);
                        return true;
                    }
                }

                return false;
            }
        };
    }

    @Bean
    public ColumnRangePartitioner partitioner() {
        return new ColumnRangePartitioner();
    }

    @Bean
    public PartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(4);
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep());
        return taskExecutorPartitionHandler;
    }

    @Bean
    public Step slaveStep() {
        return stepBuilderFactory.get("slaveStep").<Customer, Customer>chunk(250)
                .reader(reader())
                .processor(processor())
                .writer(customerWriter)
                .faultTolerant()
                .skipPolicy(incorrectTokenSkipPolicy())
                .build();
    }

    @Bean
    public Step masterStep() {
        return stepBuilderFactory.get("masterStep")
                .partitioner(slaveStep().getName(), partitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomers")
                .flow(masterStep()).end().build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        return taskExecutor;
    }
}