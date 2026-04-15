package com.sofka.worker.repository;

import com.sofka.worker.entity.OutboxEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface OutboxEventJpaRepository extends JpaRepository<OutboxEventEntity, UUID> {

    @Query("""
            SELECT o FROM OutboxEventEntity o
            WHERE o.processed = false
              AND (o.nextRetryAt IS NULL OR o.nextRetryAt <= :now)
            ORDER BY o.createdAt ASC
            """)
    List<OutboxEventEntity> findPendingEvents(LocalDateTime now, Pageable pageable);
}
