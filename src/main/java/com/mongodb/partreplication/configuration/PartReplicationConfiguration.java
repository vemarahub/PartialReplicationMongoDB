package com.mongodb.partreplication.configuration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.partreplication.dto.TokenCollection;
import lombok.RequiredArgsConstructor;  // Injects MongoProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties;  // Enables the POJO
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;

@Configuration
@EnableConfigurationProperties(MongoProperties.class)  // Registers the properties bean
@RequiredArgsConstructor
public class PartReplicationConfiguration {
    private final MongoProperties mongoProperties;  // Injected via constructor

    @Bean(name = "sourceMongoClient")
    @Primary
    public MongoClient sourceMongoClient() {
        System.out.println("Creating sourceMongoClient with URI: " + mongoProperties.getSourceUri());  // Debug log
        return MongoClients.create(mongoProperties.getSourceUri());
    }

    @Bean(name = "targetMongoClient")
    public MongoClient targetMongoClient() {
        System.out.println("Creating targetMongoClient with URI: " + mongoProperties.getTargetUri());  // Debug log
        return MongoClients.create(mongoProperties.getTargetUri());
    }
    public List<String> getIgnoreDatabases() {
        return mongoProperties.getIgnoreDatabases();
    }

    public List<String> getIncludeDatabases() {
        return mongoProperties.getIncludeDatabases();
    }

    public TokenCollection getTokenCollection() {
        return mongoProperties.getTokenCollection();
    }
}