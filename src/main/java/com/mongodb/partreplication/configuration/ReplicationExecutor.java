package com.mongodb.partreplication.configuration;

import lombok.Getter;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Configuration
@Getter
public class ReplicationExecutor {
    private final AtomicBoolean isRunning;
    private final ExecutorService executorService;

    public ReplicationExecutor() {
        this.isRunning = new AtomicBoolean(false);
        this.executorService = Executors.newSingleThreadExecutor();
    }
}
