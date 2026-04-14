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
        List<OutboxEventEntity> pending = outboxRepo.findByProcessedFalseOrderByCreatedAtAsc(
                PageRequest.of(0, 100));

        if (pending.isEmpty()) {
            return;
        }

        log.debug("Outbox poller: {} evento(s) pendiente(s)", pending.size());

        for (OutboxEventEntity event : pending) {
            try {
                kafkaTemplate.send(topic, event.getAggregateId(), event.getPayload())
                        .get();

                event.setProcessed(true);
                event.setProcessedAt(LocalDateTime.now());
                outboxRepo.save(event);

                log.info("Evento publicado y marcado procesado: id={}, type={}, aggregateId={}",
                        event.getId(), event.getEventType(), event.getAggregateId());

            } catch (ExecutionException ex) {
                log.error("Error publicando evento id={}: {}. Se reintentará en el próximo ciclo.",
                        event.getId(), ex.getMessage());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.error("Hilo interrumpido publicando evento id={}: {}.",
                        event.getId(), ex.getMessage());
            }
        }
    }
}
