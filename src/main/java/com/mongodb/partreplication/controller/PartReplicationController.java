package com.mongodb.partreplication.controller;

import com.mongodb.partreplication.dto.ReplicationResponseDTO;
import com.mongodb.partreplication.service.PartReplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
@RequestMapping("/api/v1/replication")
@RequiredArgsConstructor
@Slf4j
public class PartReplicationController {

    private final PartReplicationService partReplicationService;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    @PostMapping("/start")
    public ResponseEntity<ReplicationResponseDTO> startReplication() {
        try {
            log.info("Starting replication process");
            partReplicationService.startReplication();

            return ResponseEntity.ok(ReplicationResponseDTO.builder()
                    .status("SUCCESS")
                    .message("Replication started successfully")
                    .timestamp(LocalDateTime.now().format(DATE_FORMATTER))
                    .build());
        } catch (Exception e) {
            log.error("Failed to start replication: {}", e.getMessage(), e);

            return ResponseEntity.internalServerError()
                    .body(ReplicationResponseDTO.builder()
                            .status("ERROR")
                            .message("Failed to start replication: " + e.getMessage())
                            .timestamp(LocalDateTime.now().format(DATE_FORMATTER))
                            .build());
        }
    }

    @PostMapping("/stop")
    public ResponseEntity<ReplicationResponseDTO> stopReplication() {
        try {
            log.info("Stopping replication process");
            partReplicationService.stopReplication();

            return ResponseEntity.ok(ReplicationResponseDTO.builder()
                    .status("SUCCESS")
                    .message("Replication stopped successfully")
                    .timestamp(LocalDateTime.now().format(DATE_FORMATTER))
                    .build());
        } catch (Exception e) {
            log.error("Failed to stop replication: {}", e.getMessage(), e);

            return ResponseEntity.internalServerError()
                    .body(ReplicationResponseDTO.builder()
                            .status("ERROR")
                            .message("Failed to stop replication: " + e.getMessage())
                            .timestamp(LocalDateTime.now().format(DATE_FORMATTER))
                            .build());
        }
    }

    @GetMapping("/status")
    public ResponseEntity<ReplicationResponseDTO> getReplicationStatus() {
        String currentStatus = partReplicationService.getStatus().name();

        return ResponseEntity.ok(ReplicationResponseDTO.builder()
                .status("SUCCESS")
                .message("Current replication status: " + currentStatus)
                .timestamp(LocalDateTime.now().format(DATE_FORMATTER))
                .build());
    }

    @GetMapping("/health")
    public ResponseEntity<ReplicationResponseDTO> healthCheck() {
        return ResponseEntity.ok(ReplicationResponseDTO.builder()
                .status("UP")
                .message("Replication service is up")
                .timestamp(LocalDateTime.now().format(DATE_FORMATTER))
                .build());
    }
}
