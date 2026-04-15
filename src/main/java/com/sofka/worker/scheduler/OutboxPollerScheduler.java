package com.sofka.worker.scheduler;

import com.sofka.worker.entity.OutboxEventEntity;
import com.sofka.worker.repository.OutboxEventJpaRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
public class OutboxPollerScheduler {

    private final OutboxEventJpaRepository outboxRepo;
    private final OutboxEventProcessor eventProcessor;
    private final String topic;

    public OutboxPollerScheduler(OutboxEventJpaRepository outboxRepo,
                                  OutboxEventProcessor eventProcessor,
                                  @Value("${kafka.topics.customer-events}") String topic) {
        this.outboxRepo = outboxRepo;
        this.eventProcessor = eventProcessor;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${outbox.poll.interval-ms:5000}")
    public void pollAndPublish() {
        List<OutboxEventEntity> pending = outboxRepo.findPendingEvents(
                LocalDateTime.now(), PageRequest.of(0, 100));

        if (pending.isEmpty()) {
            return;
        }

        log.debug("Outbox poller: {} evento(s) pendiente(s)", pending.size());

        for (OutboxEventEntity event : pending) {
            eventProcessor.publishEvent(event, topic);
        }
    }
}
