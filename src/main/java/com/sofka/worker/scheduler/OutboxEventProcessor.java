package com.sofka.worker.scheduler;

import com.sofka.worker.entity.OutboxEventEntity;
import com.sofka.worker.repository.OutboxEventJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventProcessor {

    private static final int MAX_RETRIES = 10;
    private static final long BASE_DELAY_SECONDS = 30L;

    private final OutboxEventJpaRepository outboxRepo;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishEvent(OutboxEventEntity event, String topic) {
        try {
            kafkaTemplate.send(topic, event.getAggregateId(), event.getPayload())
                    .get();

            event.setProcessed(true);
            event.setProcessedAt(LocalDateTime.now());
            event.setLastError(null);
            outboxRepo.save(event);

            log.info("Evento publicado: id={}, type={}, aggregateId={}",
                    event.getId(), event.getEventType(), event.getAggregateId());

        } catch (ExecutionException ex) {
            handleFailure(event, ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            handleFailure(event, "Interrupted: " + ex.getMessage());
        }
    }

    private void handleFailure(OutboxEventEntity event, String errorMsg) {
        event.setRetryCount(event.getRetryCount() + 1);
        event.setLastError(truncate(errorMsg));

        if (event.getRetryCount() >= MAX_RETRIES) {
            event.setProcessed(true);
            event.setProcessedAt(LocalDateTime.now());
            log.error("Evento agotó {} reintentos, marcado como fallido: id={}, error={}",
                    MAX_RETRIES, event.getId(), errorMsg);
        } else {
            long delay = BASE_DELAY_SECONDS * (1L << event.getRetryCount());
            event.setNextRetryAt(LocalDateTime.now().plusSeconds(delay));
            log.warn("Reintento {}/{} para evento id={}. Próximo intento: {}. Error: {}",
                    event.getRetryCount(), MAX_RETRIES,
                    event.getId(), event.getNextRetryAt(), errorMsg);
        }

        outboxRepo.save(event);
    }

    private String truncate(String text) {
        if (text == null) {
            return null;
        }
        final int maxLength = 500;
        return text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
