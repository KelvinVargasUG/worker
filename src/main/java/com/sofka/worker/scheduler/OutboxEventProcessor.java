package com.sofka.worker.scheduler;

import com.sofka.worker.entity.OutboxEventEntity;
import com.sofka.worker.repository.OutboxEventJpaRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class OutboxEventProcessor {

    private final OutboxEventJpaRepository outboxRepo;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int maxRetries;
    private final long baseDelaySeconds;
    private final long sendTimeoutSeconds;

    public OutboxEventProcessor(OutboxEventJpaRepository outboxRepo,
                                KafkaTemplate<String, String> kafkaTemplate,
                                @Value("${outbox.retry.max-retries:10}") int maxRetries,
                                @Value("${outbox.retry.base-delay-seconds:30}") long baseDelaySeconds,
                                @Value("${outbox.send.timeout-seconds:10}") long sendTimeoutSeconds) {
        this.outboxRepo = outboxRepo;
        this.kafkaTemplate = kafkaTemplate;
        this.maxRetries = maxRetries;
        this.baseDelaySeconds = baseDelaySeconds;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishEvent(OutboxEventEntity event, String topic) {
        try {
            kafkaTemplate.send(topic, event.getAggregateId(), event.getPayload())
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);

            event.setProcessed(true);
            event.setProcessedAt(LocalDateTime.now());
            event.setLastError(null);
            outboxRepo.save(event);

            log.info("Evento publicado: id={}, type={}, aggregateId={}",
                    event.getId(), event.getEventType(), event.getAggregateId());

        } catch (ExecutionException ex) {
            handleFailure(event, ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
        } catch (TimeoutException ex) {
            handleFailure(event, "Timeout tras " + sendTimeoutSeconds + "s esperando respuesta de Kafka");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            handleFailure(event, "Interrupted: " + ex.getMessage());
        }
    }

    private void handleFailure(OutboxEventEntity event, String errorMsg) {
        event.setRetryCount(event.getRetryCount() + 1);
        event.setLastError(truncate(errorMsg));

        if (event.getRetryCount() >= maxRetries) {
            event.setProcessed(true);
            event.setProcessedAt(LocalDateTime.now());
            log.error("Evento agotó {} reintentos, marcado como fallido: id={}, error={}",
                    maxRetries, event.getId(), errorMsg);
        } else {
            long delay = baseDelaySeconds * (1L << event.getRetryCount());
            event.setNextRetryAt(LocalDateTime.now().plusSeconds(delay));
            log.warn("Reintento {}/{} para evento id={}. Próximo intento: {}. Error: {}",
                    event.getRetryCount(), maxRetries,
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
