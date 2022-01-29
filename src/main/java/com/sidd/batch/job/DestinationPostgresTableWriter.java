package com.sidd.batch.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author siddarth.ajay
 */

@Component
@Slf4j
public class DestinationPostgresTableWriter implements ItemWriter<DestinationPostgresEntity> {


    private final PostgresDestinationRepository postgresDestinationRepository;

    @Autowired
    public DestinationPostgresTableWriter(PostgresDestinationRepository postgresDestinationRepository) {
        this.postgresDestinationRepository = postgresDestinationRepository;
    }

    @Override
    public void write(List<? extends DestinationPostgresEntity> destinationPostgresEntityList) {
        log.info("Entering DestinationPostgresTableWriter.write() with destinationPostgresEntityList.size() = {}", destinationPostgresEntityList.size());
        for (DestinationPostgresEntity destinationPostgresEntity : destinationPostgresEntityList) {
            postgresDestinationRepository.save(destinationPostgresEntity);
        }
        log.info("Leaving DestinationPostgresTableWriter.write()............................");
    }
}
