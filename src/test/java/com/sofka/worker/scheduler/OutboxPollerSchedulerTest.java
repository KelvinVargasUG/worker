package com.sofka.worker.scheduler;

import com.sofka.worker.entity.OutboxEventEntity;
import com.sofka.worker.repository.OutboxEventJpaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPollerSchedulerTest {

    @Mock
    OutboxEventJpaRepository outboxRepo;

    @Mock
    OutboxEventProcessor eventProcessor;

    OutboxPollerScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new OutboxPollerScheduler(outboxRepo, eventProcessor, "customer-events");
    }

    @Test
    void shouldDoNothingWhenNoPendingEvents() {
        when(outboxRepo.findPendingEvents(any(), any())).thenReturn(List.of());

        scheduler.pollAndPublish();

        verify(eventProcessor, never()).publishEvent(any(), any());
    }

    @Test
    void shouldDelegateEachEventToProcessor() {
        OutboxEventEntity event1 = buildEvent();
        OutboxEventEntity event2 = buildEvent();
        event2.setAggregateId("agg-2");

        when(outboxRepo.findPendingEvents(any(), any())).thenReturn(List.of(event1, event2));

        scheduler.pollAndPublish();

        verify(eventProcessor).publishEvent(event1, "customer-events");
        verify(eventProcessor).publishEvent(event2, "customer-events");
        verify(eventProcessor, times(2)).publishEvent(any(), eq("customer-events"));
    }

    private OutboxEventEntity buildEvent() {
        OutboxEventEntity event = new OutboxEventEntity();
        event.setId(UUID.randomUUID());
        event.setAggregateId("agg-1");
        event.setEventType("CREATED");
        event.setPayload("{\"data\":1}");
        event.setCreatedAt(LocalDateTime.now());
        event.setProcessed(false);
        event.setRetryCount(0);
        return event;
    }
}
