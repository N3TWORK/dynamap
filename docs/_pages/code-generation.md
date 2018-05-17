---
title: "Code Generation"
permalink: /code-generation/
---

## Adding Dynamap as a dependency

Dynamap is available on the central maven repository.

In your Maven project file add the following dependency:

```xml
<dependency>
    <groupId>com.n3twork.dynamap</groupId>
    <artifactId>dynamap</artifactId>
    <version>{{ page.dynamap_version }}</version>
</dependency>
```


## Calling the code generator directly from command line

Dynamap provides a class, `com.n3twork.dynamap.CodeGenerator` that generates the code. 
To execute the generator from the command line you pass one or more arguments named `schema`, and named argument `output` that

```bash
$ java -jar <path-to-jar>/dynamap-<version>.jar --schema <path-to-schema1.json> --schema <path-to-schema2.json> --output <output path>
```

## Attaching the code generator to the Maven lifecycle

It is much easier to integrate code generation into your maven build.

Attach the code generator on your schema file and bind this execution to the `generate-sources` Maven build phase:

```xml
  <plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <executions>
        <execution>
            <id>generate-source-files</id>
            <phase>generate-sources</phase>
            <goals>
                <goal>java</goal>
            </goals>
            <configuration>
                <mainClass>com.n3twork.dynamap.CodeGenerator</mainClass>
                <arguments>
                    <argument>--schema</argument>
                    <argument>${project.basedir}/src/main/resources/<your-schema-file1>.json</argument>
                    <argument>--schema</argument>
                    <argument>${project.basedir}/src/main/resources/<your-schema-file2>.json</argument>
                    <argument>--output</argument>
                    <argument>${project.build.directory}/generated-sources/dynamap/</argument>
                </arguments>
            </configuration>
        </execution>
    </executions>
  </plugin>
```

The maven-build-helper can add these generated sources to your classpath:

```xml
    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>build-helper-maven-plugin</artifactId>
                <version>1.8</version>
                <executions>
                    <execution>
                        <id>add-source</id>
                        <phase>generate-test-sources</phase>
                        <goals>
                            <goal>add-source</goal>
                        </goals>
                        <configuration>
                            <sources>
                                <source>${project.build.directory}/generated-sources/dynamap/</source>
                            </sources>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
         </plugins>
     </build>
```