package com.example.demo.config;

import java.util.List;
import java.util.concurrent.CompletableFuture;


import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;

import com.example.demo.exception.ChangeStreamException;
import com.example.demo.model.Customer;
import com.example.demo.model.Lang;
import com.example.demo.model.Name;
import com.example.demo.model.Policy;
import com.example.demo.model.Searchable;
import com.example.demo.model.SearchableType;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;

import jakarta.annotation.PostConstruct;

@Configuration
public class ChangeStreamConfig {

    private final String COLL_CUSTOMER = "customer";
    private final String COLL_POLICY = "policy";
    private final String COLL_SEARCH = "search";
    private final String COLL_TEST = "test";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    ConversionService conversionService;

    @Autowired
    CodecRegistry codecRegistry;

    String resumeTokenString;

    @PostConstruct
    private void startChangeStream() {

        CompletableFuture.supplyAsync(() -> {
            logger.info("start consumer change stream");
            start();
            startColl();
            return null;
        });
    }

    @Retryable(include = ChangeStreamException.class, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    private void start() {
        ChangeStreamIterable<Document> changeStream = mongoTemplate.getDb().watch(List.of(Aggregates.match(
                Filters.in("ns.coll", List.of(COLL_CUSTOMER, COLL_POLICY))
        ))).fullDocument(FullDocument.UPDATE_LOOKUP);
        if (resumeTokenString != null) {
            BsonDocument resumeToken = new BsonDocument();
            resumeToken.put("_data", new BsonString(resumeTokenString));
            changeStream.resumeAfter(resumeToken);
        }
        changeStream.forEach(e -> {
            logger.info("update event received: " + e);
            resumeTokenString = e.getResumeToken().getString("_data").toString();
            if (OperationType.INVALIDATE == e.getOperationType()) {
                throw new ChangeStreamException();
            } else {
//TODO: pre-calculatiuon for agent view
                try {
                    MongoCollection<Searchable> c = mongoTemplate.getDb().withCodecRegistry(codecRegistry).getCollection(COLL_SEARCH, Searchable.class);
                    Searchable searchable = new Searchable();
                    searchable.setId(e.getDocumentKey().getObjectId("_id").getValue());
                    switch (e.getOperationType()) {
                        case INSERT:
                        case UPDATE:
                        case REPLACE:
                            switch (e.getNamespace().getCollectionName()) {
                                case COLL_CUSTOMER:
                                    searchable.setType(SearchableType.CUSTOMER);
                                    Customer customer = codecRegistry.get(Customer.class).decode(e.getFullDocument().toBsonDocument().asBsonReader(), DecoderContext.builder().build());
                                    searchable.setName(customer.getName());
                                    break;
                                case COLL_POLICY:
                                    searchable.setType(SearchableType.POLICY);
                                    Policy p = codecRegistry.get(Policy.class).decode(e.getFullDocument().toBsonDocument().asBsonReader(), DecoderContext.builder().build());
                                    searchable.setName(Name.builder().en(findLangValue(p.getName(), "en")).zhhk(findLangValue(p.getName(), "zh-hk")).zhcn(findLangValue(p.getName(), "zh-cn")).build());
                                    break;
                            }
                            logger.info("save: " + c.replaceOne(Filters.eq("_id", searchable.getId()), searchable, new ReplaceOptions().upsert(true)));
                            break;
                        case DELETE:
                            logger.info("delete: " + c.deleteOne(Filters.eq("_id", searchable.getId())));
                            break;
                        default:
                            break;
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
    }

    private String findLangValue(List<Lang> ls, String langCode) {
        return ls.stream().filter(l -> langCode.equalsIgnoreCase(l.getLang())).map(l -> l.getValue()).findFirst().orElse(null);
    }

    @Recover
    public void changeStreamException(ChangeStreamException e) {
        logger.error("Retry failure at: " + resumeTokenString);
    }

    @Retryable(include = ChangeStreamException.class, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    private void startColl() {
        MongoCollection<Document> coll = mongoTemplate.getDb().getCollection(COLL_TEST);
        ChangeStreamIterable<Document> changeStream = coll.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
        if (resumeTokenString != null) {
            BsonDocument resumeToken = new BsonDocument();
            resumeToken.put("_data", new BsonString(resumeTokenString));
            changeStream.resumeAfter(resumeToken);
        }
        changeStream.forEach(e -> {
            logger.info("update event received: " + e);
            resumeTokenString = e.getResumeToken().getString("_data").toString();
            if (OperationType.INVALIDATE == e.getOperationType()) {
                throw new ChangeStreamException();
            } else {
                //do nothing
            }
        });
    }
}
