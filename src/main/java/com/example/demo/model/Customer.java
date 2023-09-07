package com.example.demo.model;

import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.springframework.data.annotation.Id;

import lombok.Data;

@Data
public class Customer {
    @Id
    @BsonId()
    @BsonRepresentation(BsonType.OBJECT_ID)
    String id;
    Name name;
}