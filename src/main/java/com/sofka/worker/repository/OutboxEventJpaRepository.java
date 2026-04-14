package com.sofka.worker.repository;

import com.sofka.worker.entity.OutboxEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventJpaRepository extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByProcessedFalseOrderByCreatedAtAsc(Pageable pageable);
}
