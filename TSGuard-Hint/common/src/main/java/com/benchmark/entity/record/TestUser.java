package com.benchmark.entity.record;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "test_users")
@Data
public class TestUser {
    @Id
    // @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", nullable = false, unique = true)
    private String id;

    @Column(name = "iname", nullable = false)
    String name;

    /*@PrePersist
    void setUp() {
        this.id = UUID.randomUUID().toString();
    }*/
}
