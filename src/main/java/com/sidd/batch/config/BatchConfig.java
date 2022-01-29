package com.sidd.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.sidd.batch.job.ConsoleItemWriter;
import com.sidd.batch.job.Customer;

import lombok.RequiredArgsConstructor;

/**
 * @author siddarth.ajay
 */

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfig {
	
	
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;

	@Value("file://C:/Users/siddarth.ajay/data/out/*")
	private Resource[] inputResources;

	@Bean
	public Job readCSVFilesJob() {
		return jobBuilderFactory.get("readCSVFilesJob")
				.incrementer(new RunIdIncrementer())
				.start(step1())
				.build();
	}
	
	@Bean("batch_step1")
	public Step step1() {
		return stepBuilderFactory.get("batch_step1")
				.<Customer, Customer>chunk(100)
				.writer(writer())
				.reader(multiResourceItemReader())
				.faultTolerant()
			      .skip(Exception.class)
			      .skipLimit(Integer.MAX_VALUE)
				.build();
	}

	@Bean("batch_multiResourceItemReader")
	public MultiResourceItemReader<Customer> multiResourceItemReader() {
		var resourceItemReader = new MultiResourceItemReader<Customer>();
		
		resourceItemReader.setResources(inputResources);
		resourceItemReader.setDelegate(reader());
		return resourceItemReader;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean("batch_reader")
	public FlatFileItemReader<Customer> reader() {
		// Create reader instance
		var reader = new FlatFileItemReader<Customer>();

		// Set number of lines to skips. Use it if file has header rows.
		//reader.setLinesToSkip(1);

		// Configure how each line will be parsed and mapped to different values
		reader.setLineMapper(new DefaultLineMapper() {
			{
				// 3 columns in each row
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "id", "firstName", "lastName", "gender", "dob", "status","empNumber" });
					}
				});
				// Set values in Employee class
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Customer>() {
					{
						setTargetType(Customer.class);
					}
				});
			}
		});
		return reader;
	}

	@Bean
	public ConsoleItemWriter<Customer> writer() {
		return new ConsoleItemWriter<>();
	}
}
