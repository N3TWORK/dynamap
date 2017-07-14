/*
    Copyright 2017 N3TWORK INC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.n3twork.dynamap.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Type {

    private final String name;
    private final List<Field> fields;
    private final List<String> hashCodeFields;
    private final List<String> equalsFields;

    @JsonCreator
    public Type(@JsonProperty("Name") String name, @JsonProperty("fields") List<Field> fields,
                @JsonProperty("hashCodeFields") List<String> hashCodeFields, @JsonProperty("equalsFields") List<String> equalsFields) {
        this.name = name;
        this.fields = fields;
        this.hashCodeFields = hashCodeFields;
        this.equalsFields = equalsFields;
    }

    public String getName() {
        return name;
    }

    public List<Field> getFields() {
        return fields;
    }

    @JsonIgnore
    public List<Field> getPersistedFields() {
        return fields.stream().filter(f -> f.isPersisted()).collect(Collectors.toList());
    }

    public List<String> getHashCodeFields() {
        return hashCodeFields;
    }

    public List<String> getEqualsFields() {
        return equalsFields;
    }
}
