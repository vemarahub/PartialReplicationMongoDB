package com.mongodb.partreplication.configuration;

import com.mongodb.partreplication.dto.TokenCollection;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;


@Getter
@AllArgsConstructor  // Lombok generates constructor with all final fields
@ConfigurationProperties(prefix = "spring.data.mongodb")  // Matches your properties prefix
public class MongoProperties {
    private final String sourceUri;
    private final String targetUri;
    private final List<String> ignoreDatabases;
    private final List<String> includeDatabases;
    private final TokenCollection tokenCollection;

    @PostConstruct
    public void logProperties() {
        System.out.println("MongoProperties bound: sourceUri=" + sourceUri + ", targetUri=" + targetUri + ", includeDatabases=" + includeDatabases);
    }
}

