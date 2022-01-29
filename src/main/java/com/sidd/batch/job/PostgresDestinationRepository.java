package com.sidd.batch.job;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PostgresDestinationRepository extends JpaRepository<DestinationPostgresEntity, Integer> {
}
