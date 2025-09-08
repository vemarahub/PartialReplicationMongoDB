package com.mongodb.partreplication.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReplicationResponseDTO {
    private String status;
    private String message;
    private String timestamp;
}
