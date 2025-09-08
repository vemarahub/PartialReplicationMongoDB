package com.mongodb.partreplication.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TokenCollection {
    private final String database;
    private final String collection;
    private final long size;
}