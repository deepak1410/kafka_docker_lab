package com.dpk.peopleconsumer.entities;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Person {
    private String id;
    private String name;
    private String title;
}
