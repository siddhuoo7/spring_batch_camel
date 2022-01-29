package com.sidd.batch.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

/**
 * @author siddarth.ajay
 */

@Component
@Slf4j
public class DataProcessor implements ItemProcessor<Customer, DestinationPostgresEntity> {

 @Override
 public DestinationPostgresEntity process(Customer customer) {
  log.info("Entering DataProcessor.process() with sourceMySqlEntity = {}", customer);

  DestinationPostgresEntity destinationPostgresEntity = new DestinationPostgresEntity();
  destinationPostgresEntity.setCustomerId(customer.getId());
  destinationPostgresEntity.setFirstName(customer.getFirstName());
  destinationPostgresEntity.setLastName(customer.getLastName());

  log.info("Leaving DataProcessor.process() with destinationPostgresEntity = {}", destinationPostgresEntity);
  return destinationPostgresEntity;
 }

}