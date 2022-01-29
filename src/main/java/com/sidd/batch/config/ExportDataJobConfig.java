package com.sidd.batch.config;

import java.net.MalformedURLException;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.sidd.batch.job.Customer;
import com.sidd.batch.job.DataProcessor;
import com.sidd.batch.job.DestinationPostgresEntity;
import com.sidd.batch.job.DestinationPostgresTableWriter;
import com.sidd.batch.job.JobCompletionNotificationListener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author siddarth.ajay
 */

@Configuration
@EnableBatchProcessing
@Slf4j
@RequiredArgsConstructor
public class ExportDataJobConfig {

	public final JobBuilderFactory jobBuilderFactory;
	public final StepBuilderFactory stepBuilderFactory;
	private final DataProcessor dataProcessor;
	private final DestinationPostgresTableWriter destinationPostgresTableWriter;
	private final FlatFileItemReader<Customer> personItemReader;
	
	@Value("file://C:/Users/siddarth.ajay/data/out/*")
	private Resource[] inputResources;
	
	@Value("${chunk_size: 15}")
	private Integer chunkSize;

	@Bean("partitioner")
	@StepScope
	public Partitioner partitioner() {
		log.info("In Partitioner");

		MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
//		ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
//		Resource[] resources = null
//			resources = resolver.getResources(outputFilePath+"*.csv")
		partitioner.setResources(inputResources);
		partitioner.partition(10);
		return partitioner;
	}

	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(masterStep())
				.end()
				.build();
	}

	@Bean
	@Qualifier("masterStep")
	public Step masterStep() {
		return stepBuilderFactory.get("masterStep")
				.partitioner("step1", partitioner())
				.step(step1())
				.taskExecutor(taskExecutor())
				.build();
	}

	@Bean
	public ThreadPoolTaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(10);
		taskExecutor.setCorePoolSize(10);
		taskExecutor.setQueueCapacity(10);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	/**
	 * 
	 */

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.<Customer, DestinationPostgresEntity>chunk(chunkSize)
				.processor(dataProcessor)
				.writer(destinationPostgresTableWriter)
				.reader(personItemReader)
				.faultTolerant()
			      .skip(Exception.class)
			      .skipLimit(Integer.MAX_VALUE)
			      .listener(new StepExecutionListener()
			          {
			            @Override
			            public void beforeStep(final StepExecution stepExecution)
			            {
			              log.info("start time : {}",stepExecution.getStartTime());
			            }

			            @Override
			            public ExitStatus afterStep(final StepExecution stepExecution)
			            {
			        	  log.info("---------------");
			        	  log.info("Step: Total records Read: {}", stepExecution.getReadCount());
			        	  log.info("Step: Total records Valid: {}", stepExecution.getWriteCount());
			        	  log.info("Step: Skip invalid: {}", stepExecution.getReadSkipCount());
			        	  log.info("---------------");
			              return stepExecution.getExitStatus();
			            }
			          })
				.build();
	}

	@Bean
	@StepScope
	@Qualifier("personItemReader")
	@DependsOn("partitioner")
	public FlatFileItemReader<Customer> personItemReader(@Value("#{stepExecutionContext['fileName']}") String filename)
			throws MalformedURLException {
		log.info("In Reader");
		return new FlatFileItemReaderBuilder<Customer>().name("personItemReader").delimited()
				.names("id", "firstName", "lastName", "gender", "dob", "status", "empNumber")
				.fieldSetMapper(new BeanWrapperFieldSetMapper<>() {
					{
						setTargetType(Customer.class);
					}
				}).resource(new UrlResource(filename)).build();
	}

//    @Bean
//    @StepScope
//    public FlatFileItemReader<Customer> personReader(
//            @Value("#{jobParameters['input.file.name']}") final String inputFileName) {
//        log.info("Importing from {}", inputFileName);
//        return new FlatFileItemReaderBuilder<Customer>().name("personItemReader")
//                .resource(new PathResource(inputFileName))
//                .delimited()
//                .names("id", "firstName", "lastName", "gender", "dob", "status", "salary", "empNumber")
//                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {
//                    {
//                        setTargetType(Customer.class);
//                    }
//                })
//                .build();
//    }
//	
//	  @Bean
//	  public JdbcBatchItemWriter<Person> personWriter(final DataSource dataSource)
//	  {
//	    return new JdbcBatchItemWriterBuilder<Person>()
//	      .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
//	      .sql("INSERT INTO person (id_person, name, cpf) VALUES (:id, :name, :cpf)")
//	      .dataSource(dataSource)
//	      .build();
//	  }

//    @Bean
//    public Step exportData() {
//        log.info("Entering ExportDataJobConfig.exportData()...........");
//        return stepBuilderFactory.get("exportDataStep")
//                .<Customer, DestinationPostgresEntity>chunk(chunkSize)
//                .reader(personReader("input.txt"))
//                .processor(dataProcessor)
//                .writer(destinationPostgresTableWriter)
//                .build();
//    }

//    @Bean
//    public Job copyCustJob(JobCompletionNotificationListener listener, Step step1) {
//        return jobBuilderFactory
//                .get("exportDataJobChild1")
//                .incrementer(new RunIdIncrementer())
//                .flow(cleanupDestinationTable())
//                .next(exportData())
//                .end().listener(listener)
//                .build();
//    }

}