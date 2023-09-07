package com.example.demo.model;

import org.bson.types.ObjectId;


import lombok.Data;

@Data
public class Searchable {
    ObjectId id;
    Name name;
    SearchableType type;
}
