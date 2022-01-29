package com.sidd.batch.job;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @author siddarth.ajay
 */

@Component
@Slf4j
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

//	@Autowired
//	private JdbcTemplate jdbcTemplate

	@Autowired
	private PostgresDestinationRepository repo;

	@Override
	public void beforeJob(JobExecution jobExecution) {
		super.beforeJob(jobExecution);
		log.info("JOB STARTED! @ {} ", jobExecution.getStartTime());
		log.info("Truncating table customer");
		repo.deleteAll();
//		jdbcTemplate.update("truncate table customer");
		log.info("Truncate successfully executed!");
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("JOB FINISHED! @ {}", jobExecution.getEndTime());

			var diffInMills = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();
			var diffInSeconds = TimeUnit.SECONDS.convert(diffInMills, TimeUnit.MILLISECONDS);
			log.info("job duration = {}", diffInSeconds);
		}
	}
}