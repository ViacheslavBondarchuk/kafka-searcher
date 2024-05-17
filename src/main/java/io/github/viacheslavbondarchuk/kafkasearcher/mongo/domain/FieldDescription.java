package io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Set;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.System.FIELDS;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 4:02 PM
 **/

@Document(collection = FIELDS)
public final class FieldDescription {
    @Id
    private String id;
    private String name;
    private Set<String> collections;
    private IndexDescription indexDescription;

    public void addCollection(String collection) {
        collections.add(collection);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getCollections() {
        return collections;
    }

    public void setCollections(Set<String> collections) {
        this.collections = collections;
    }

    public IndexDescription getIndexDescription() {
        return indexDescription;
    }

    public void setIndexDescription(IndexDescription indexDescription) {
        this.indexDescription = indexDescription;
    }

    @Override
    public String toString() {
        return "FieldDescription[" +
               "id=" + id + ", " +
               "name=" + name + ", " +
               "collections=" + collections + ", " +
               "indexDescription=" + indexDescription + ']';
    }

}
