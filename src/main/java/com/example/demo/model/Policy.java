package com.example.demo.model;

import java.util.List;

import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.DocumentReference;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class Policy {
    @Id
    @BsonId()
    @BsonRepresentation(BsonType.OBJECT_ID)
    String id;

    @DocumentReference
    @BsonIgnore
    Customer customer;

    @Transient
    @JsonIgnore
    @BsonProperty("customer")
    @BsonRepresentation(BsonType.OBJECT_ID)
    String customerId;

    List<Lang> name;
}