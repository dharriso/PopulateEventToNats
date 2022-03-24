package com.optiva.tools.addevents;

import com.dslplatform.json.*;
import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.runtime.Settings;

import java.io.ByteArrayOutputStream;

@CompiledJson
public class Person {

    public final String firstName, lastName;
    public final int age;

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }

    public Person(String firstName, String lastName, int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }
/*
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String firstName, lastName;
        private int age;

        private Builder() {}

        public Builder firstName(String firstName) {
            this.firstName = firstName;
            return this;
        }

        public Builder lastName(String lastName) {
            this.lastName = lastName;
            return this;
        }

        public Builder age(int age) {
            this.age = age;
            return this;
        }

        public PersonBuilder build() {
            return new PersonBuilder(firstName, lastName, age);
        }
    }
*/
    public static void main(String[] args) {
        //PersonBuilder event = PersonBuilder.builder().firstName("first").lastName("last").age(42).build();
        Person person = new Person("first", "second", 21);
        //DslJson<Object> mydsl =  new DslJson<>(Settings.withRuntime().includeServiceLoader());

        DslJson<Object> mydsl =  new DslJson<>(Settings.withRuntime().includeServiceLoader());
        JsonWriter writer = mydsl.newWriter();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        JsonEvent pojo = null;
        try {
            // dsl.serialize(writer, event);
            mydsl.serialize(person, output);
            byte[] input = writer.getByteBuffer();
           // pojo = mydsl.deserialize(Person.class, input, input.length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("json : "+writer.toString());
       // System.out.println("pojo : "+pojo.toString());
    }
}
