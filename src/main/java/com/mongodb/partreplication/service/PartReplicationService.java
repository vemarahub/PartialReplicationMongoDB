package com.mongodb.partreplication.service;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.partreplication.configuration.MongoProperties;
import com.mongodb.partreplication.configuration.PartReplicationConfiguration;
import com.mongodb.partreplication.configuration.ReplicationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.empty;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.not;

@Service
@RequiredArgsConstructor
@Slf4j
public class PartReplicationService {

    private final MongoClient sourceMongoClient;
    private final MongoClient targetMongoClient;
    private final PartReplicationConfiguration partReplicationConfiguration;

    private final MongoProperties mongoProperties;
    private final ReplicationExecutor replicationExecutor;

    @Getter
    private ReplicationStatus status = ReplicationStatus.STOPPED;
    private Future<?> replicationTask;

    private void storeResumeToken(ChangeStreamDocument<Document> change) {
        if (change != null && change.getResumeToken() != null) {
            try {
                BsonDocument resumeToken = change.getResumeToken();
                String tokenDb = mongoProperties.getTokenCollection().getDatabase();
                String tokenColl = mongoProperties.getTokenCollection().getCollection();

                targetMongoClient.getDatabase(tokenDb)
                        .getCollection(tokenColl)
                        .updateOne(
                                new Document("_id", "lastResumeToken"),
                                new Document("$set", new Document("token", resumeToken)),
                                new UpdateOptions().upsert(true)
                        );

                log.debug("Stored resume token in {}.{}: {}", tokenDb, tokenColl, resumeToken);
            } catch (Exception e) {
                log.error("Failed to store resume token: {}", e.getMessage(), e);
            }
        }
    }

    // Retrieving the resume token
    private BsonDocument getLastResumeToken() {
        try {
            String tokenDb = partReplicationConfiguration.getTokenCollection().getDatabase();
            String tokenColl = partReplicationConfiguration.getTokenCollection().getCollection();

            Document tokenDoc = targetMongoClient.getDatabase(tokenDb)
                    .getCollection(tokenColl)
                    .find(new Document("_id", "lastResumeToken"))
                    .first();

            if (tokenDoc != null && tokenDoc.get("token") != null) {
                Object tokenObj = tokenDoc.get("token");
                if (tokenObj instanceof BsonDocument) {
                    log.debug("Retrieved valid resume token from {}.{}", tokenDb, tokenColl);
                    return (BsonDocument) tokenObj;
                } else {
                    log.warn("Invalid resume token type (expected BsonDocument, got {}); deleting and starting fresh.", tokenObj.getClass().getSimpleName());
                    // Clean up invalid token to avoid repeated warnings
                    targetMongoClient.getDatabase(tokenDb).getCollection(tokenColl).deleteOne(new Document("_id", "lastResumeToken"));
                }
            }
            return null;
        } catch (Exception e) {
            log.error("Failed to retrieve resume token: {}", e.getMessage(), e);
            return null;
        }
    }

    // Creating the change stream with resume token
    private ChangeStreamIterable<Document> createChangeStream(BsonDocument resumeToken) {
        try {
            List<Bson> pipeline = new ArrayList<>();
            List<String> include = partReplicationConfiguration.getIncludeDatabases();
            List<String> ignore = partReplicationConfiguration.getIgnoreDatabases();
            Bson dbFilter = empty();
            if (!include.isEmpty()) {
                dbFilter = in("ns.db", include.toArray(new String[0]));
                log.debug("Include filter for dbs: {}", include);
            }
            if (!ignore.isEmpty()) {
                Bson ignoreFilter = not(in("ns.db", ignore.toArray(new String[0])));
                dbFilter = and(dbFilter, ignoreFilter);
                log.debug("Ignore filter for dbs: {}", ignore);
            }
            if (!dbFilter.equals(empty())) {
                pipeline.add(match(dbFilter));
            }
            log.info("Change stream pipeline stages: {}", pipeline.size());
            if (!pipeline.isEmpty()) {
                log.debug("Pipeline filter details: {}", dbFilter.toBsonDocument());
            }

            ChangeStreamIterable<Document> stream = sourceMongoClient.watch(pipeline)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .maxAwaitTime(1, TimeUnit.SECONDS);

            if (resumeToken != null) {
                log.info("Resuming change stream with token: {}", resumeToken);
                stream = stream.resumeAfter(resumeToken);
            } else {
                log.info("Starting new change stream without resume token");
            }

            return stream;
        } catch (Exception e) {
            log.error("Error creating change stream: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create change stream", e);
        }
    }

    private void processChange(ChangeStreamDocument<Document> change) {
        if (change.getNamespace() == null) {
            log.debug("Skipping non-namespace change: {}", change.getOperationType());
            storeResumeToken(change);
            return;
        }

        String database = change.getNamespace().getDatabaseName();
        String collection = change.getNamespace().getCollectionName();
        log.info("Processing change: {} on {}.{}", change.getOperationType(), database, collection);

        // Skip if database should not be processed
        if (!shouldProcessDatabase(database)) {
            log.debug("Skipping change for database: {}", database);
            storeResumeToken(change);
            return;
        }

        try {
            // Process the change
            switch (change.getOperationType()) {
                case INSERT:
                    handleInsertOrUpdate(database, collection, change, true);
                    break;
                case UPDATE:
                case REPLACE:
                    handleInsertOrUpdate(database, collection, change, false);
                    break;
                case DELETE:
                    handleDelete(database, collection, change);
                    break;
                case DROP:
                    handleDrop(database, collection);
                    break;
                default:
                    log.debug("Unhandled operation type: {} for {}.{}", change.getOperationType(), database, collection);
            }

            // Store resume token after successful processing
            storeResumeToken(change);
        } catch (Exception e) {
            log.error("Error processing change for database {}: {}", database, e.getMessage(), e);
        }
    }

    private boolean shouldProcessDatabase(String database) {
        return partReplicationConfiguration.getIncludeDatabases().contains(database) &&
                !partReplicationConfiguration.getIgnoreDatabases().contains(database);
    }

    public synchronized void startReplication() {
        if (replicationExecutor.getIsRunning().get()) {
            log.warn("Replication is already running");
            return;
        }

        replicationExecutor.getIsRunning().set(true);
        status = ReplicationStatus.STARTING;

        replicationTask = replicationExecutor.getExecutorService().submit(() -> {
            try {
                status = ReplicationStatus.RUNNING;
                BsonDocument resumeToken = getLastResumeToken();
                var stream = createChangeStream(resumeToken);

                log.info("Starting to iterate change stream...");
                stream.forEach(change -> {
                    log.debug("Received change event: {}", change.getOperationType());
                    try {
                        if (!replicationExecutor.getIsRunning().get()) {
                            log.info("Replication stopped; exiting loop");
                            return;
                        }
                        processChange(change);
                    } catch (Exception e) {
                        log.error("Error processing change: ", e);
                    }
                });
                log.info("Change stream iteration ended (stream closed or error)");

            } catch (Exception e) {
                log.error("Fatal error in replication", e);
                status = ReplicationStatus.ERROR;
            } finally {
                status = ReplicationStatus.STOPPED;
                replicationExecutor.getIsRunning().set(false);
            }
        });
    }

    public synchronized void stopReplication() {
        if (!replicationExecutor.getIsRunning().get()) {
            log.warn("Replication is not running");
            return;
        }

        log.info("Stopping replication");
        replicationExecutor.getIsRunning().set(false);
        if (replicationTask != null) {
            replicationTask.cancel(true);
        }
    }

    private void handleInsertOrUpdate(String database, String collection, ChangeStreamDocument<Document> change, boolean isInsert) {
        try {
            Document document = change.getFullDocument();
            if (document == null) {
                log.warn("No full document for operation {} in {}.{}", change.getOperationType(), database, collection);
                return;
            }

            // Ensure collection exists on target
            ensureCollectionExists(database, collection);

            BsonDocument keyFilter = change.getDocumentKey();
            log.info("About to {} in {}.{}: filter={}, doc={}",
                    isInsert ? "insert" : "replace", database, collection,
                    keyFilter.toJson(), document.toJson());
            log.info("Target client cluster hosts: {}", targetMongoClient.getClusterDescription().getClusterSettings().getHosts());
            log.info("Target client connected to: {}", targetMongoClient.getDatabase("admin").runCommand(new Document("ismaster", 1)));
            log.info("Target client isMaster response: {}", " result from runCommand ");
            if (isInsert) {
                // For INSERT: Use insertOne for simplicity and explicit failure
                targetMongoClient.getDatabase(database)
                        .getCollection(collection)
                        .insertOne(document);
                log.info("Inserted document in {}.{}: _id={}", database, collection, document.get("_id"));
            } else {
                // For UPDATE/REPLACE: Use replaceOne without upsert
                ReplaceOptions options = new ReplaceOptions().upsert(false);
                UpdateResult result = targetMongoClient.getDatabase(database)
                        .getCollection(collection)
                        .replaceOne(keyFilter, document, options);
                log.info("replaceOne RESULT in {}.{}: matched={}, modified={}",
                        database, collection, result.getMatchedCount(), result.getModifiedCount());
                if (result.getMatchedCount() == 0) {
                    log.warn("Update failed: No matching document for filter={}", keyFilter.toJson());
                }
            }
        } catch (Exception e) {
            log.error("Failed to handle {} in {}.{}: {}",
                    isInsert ? "insert" : "update/replace", database, collection, e.getMessage(), e);
            e.printStackTrace(); // Temporary: Full stack trace for debugging
        }
    }

    private void handleDelete(String database, String collection, ChangeStreamDocument<Document> change) {
        try {
            ensureCollectionExists(database, collection); // Ensure collection exists for deletes
            BsonDocument keyFilter = change.getDocumentKey();
            DeleteResult result = targetMongoClient.getDatabase(database)
                    .getCollection(collection)
                    .deleteOne(keyFilter);
            log.info("Deleted document in {}.{}: filter={}, deletedCount={}",
                    database, collection, keyFilter.toJson(), result.getDeletedCount());
            if (result.getDeletedCount() == 0) {
                log.warn("Delete failed: No matching document for filter={}", keyFilter.toJson());
            }
        } catch (Exception e) {
            log.error("Failed to delete document in {}.{}: {}", database, collection, e.getMessage(), e);
        }
    }

    private void handleDrop(String database, String collection) {
        try {
            targetMongoClient.getDatabase(database)
                    .getCollection(collection)
                    .drop();
            log.info("Dropped collection {}.{}", database, collection);
        } catch (Exception e) {
            log.error("Failed to drop collection {}.{}: {}", database, collection, e.getMessage(), e);
        }
    }

    private void ensureCollectionExists(String database, String collection) {
        try {
            // Check if database exists by listing collections
            List<String> collections = targetMongoClient.getDatabase(database)
                    .listCollectionNames()
                    .into(new ArrayList<>());
            if (!collections.contains(collection)) {
                log.info("Creating collection {}.{} on target", database, collection);
                targetMongoClient.getDatabase(database).createCollection(collection);
            } else {
                log.debug("Collection {}.{} already exists on target", database, collection);
            }
        } catch (Exception e) {
            log.error("Failed to ensure collection {}.{} exists: {}", database, collection, e.getMessage(), e);
            // Continue to attempt the write operation; MongoDB may auto-create DB
        }
    }

    public enum ReplicationStatus {
        STOPPED,
        STARTING,
        RUNNING,
        ERROR
    }
}