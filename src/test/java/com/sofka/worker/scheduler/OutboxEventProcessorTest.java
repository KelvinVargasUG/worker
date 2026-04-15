package com.sofka.worker.scheduler;

import com.sofka.worker.entity.OutboxEventEntity;
import com.sofka.worker.repository.OutboxEventJpaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEventProcessorTest {

    @Mock
    OutboxEventJpaRepository outboxRepo;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    OutboxEventProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new OutboxEventProcessor(outboxRepo, kafkaTemplate);
    }

    @Test
    void shouldPublishAndMarkProcessedOnSuccess() {
        OutboxEventEntity event = buildEvent();
        when(kafkaTemplate.send("customer-events", "agg-1", "{\"data\":1}"))
                .thenReturn(CompletableFuture.completedFuture(null));

        processor.publishEvent(event, "customer-events");

        assertThat(event.isProcessed()).isTrue();
        assertThat(event.getProcessedAt()).isNotNull();
        assertThat(event.getLastError()).isNull();
        verify(outboxRepo).save(event);
    }

    @Test
    void shouldIncrementRetryOnKafkaFailure() {
        OutboxEventEntity event = buildEvent();
        CompletableFuture<Object> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException(new RuntimeException("Kafka down")));

        when(kafkaTemplate.send(eq("customer-events"), anyString(), anyString()))
                .thenReturn((CompletableFuture) failedFuture);

        processor.publishEvent(event, "customer-events");

        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getLastError()).contains("Kafka down");
        assertThat(event.isProcessed()).isFalse();
        assertThat(event.getNextRetryAt()).isAfter(LocalDateTime.now().plusSeconds(29));
        verify(outboxRepo).save(event);
    }

    @Test
    void shouldMarkProcessedAfterMaxRetries() {
        OutboxEventEntity event = buildEvent();
        event.setRetryCount(9);

        CompletableFuture<Object> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException(new RuntimeException("still down")));

        when(kafkaTemplate.send(eq("customer-events"), anyString(), anyString()))
                .thenReturn((CompletableFuture) failedFuture);

        processor.publishEvent(event, "customer-events");

        assertThat(event.getRetryCount()).isEqualTo(10);
        assertThat(event.isProcessed()).isTrue();
        assertThat(event.getProcessedAt()).isNotNull();
        assertThat(event.getLastError()).contains("still down");
    }

    @Test
    void shouldComputeExponentialBackoff() {
        OutboxEventEntity event = buildEvent();
        event.setRetryCount(2);

        CompletableFuture<Object> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException(new RuntimeException("error")));

        when(kafkaTemplate.send(eq("customer-events"), anyString(), anyString()))
                .thenReturn((CompletableFuture) failedFuture);

        processor.publishEvent(event, "customer-events");

        // retry_count 3 → delay = 30 * 2^3 = 240s = 4 min
        assertThat(event.getRetryCount()).isEqualTo(3);
        assertThat(event.getNextRetryAt())
                .isAfter(LocalDateTime.now().plusSeconds(200))
                .isBefore(LocalDateTime.now().plusSeconds(260));
    }

    @Test
    void shouldTruncateErrorMessageOver500Chars() {
        OutboxEventEntity event = buildEvent();
        String longError = "X".repeat(600);

        CompletableFuture<Object> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException(new RuntimeException(longError)));

        when(kafkaTemplate.send(eq("customer-events"), anyString(), anyString()))
                .thenReturn((CompletableFuture) failedFuture);

        processor.publishEvent(event, "customer-events");

        assertThat(event.getLastError()).hasSize(500);
    }

    @Test
    void shouldHandleInterruptedExceptionGracefully() {
        OutboxEventEntity event = buildEvent();
        CompletableFuture<Object> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new InterruptedException("interrupted"));

        when(kafkaTemplate.send(eq("customer-events"), anyString(), anyString()))
                .thenReturn((CompletableFuture) failedFuture);

        processor.publishEvent(event, "customer-events");

        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getLastError()).contains("interrupted");
        assertThat(event.isProcessed()).isFalse();
        verify(outboxRepo).save(event);
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
