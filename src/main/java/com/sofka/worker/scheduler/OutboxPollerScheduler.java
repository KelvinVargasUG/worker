package com.sofka.worker.scheduler;

import com.sofka.worker.entity.OutboxEventEntity;
import com.sofka.worker.repository.OutboxEventJpaRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
public class OutboxPollerScheduler {

    private static final int MAX_RETRIES = 10;
    private static final long BASE_DELAY_SECONDS = 30L;

    private final OutboxEventJpaRepository outboxRepo;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public OutboxPollerScheduler(OutboxEventJpaRepository outboxRepo,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  @Value("${kafka.topics.customer-events}") String topic) {
        this.outboxRepo = outboxRepo;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${outbox.poll.interval-ms:5000}")
    @Transactional
    public void pollAndPublish() {
        List<OutboxEventEntity> pending = outboxRepo.findPendingEvents(
                LocalDateTime.now(), PageRequest.of(0, 100));

        if (pending.isEmpty()) {
            return;
        }

        log.debug("Outbox poller: {} evento(s) pendiente(s)", pending.size());

        for (OutboxEventEntity event : pending) {
            publishEvent(event);
        }
    }

    private void publishEvent(OutboxEventEntity event) {
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
        event.setLastError(truncate(errorMsg, 500));

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

    private String truncate(String text, int maxLength) {
        if (text == null) {
            return null;
        }
        return text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
