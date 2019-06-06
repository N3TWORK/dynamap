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

package com.n3twork.dynamap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.n3twork.dynamap.model.Field;
import com.n3twork.dynamap.model.Schema;
import com.n3twork.dynamap.model.TableDefinition;
import com.n3twork.dynamap.model.Type;
import freemarker.template.Configuration;
import freemarker.template.Template;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class CodeGenerator {

    private static final String OPT_SCHEMA_FILE_PATH = "schema";
    private static final String OPT_OUTPUT_PATH = "output";

    private static final Set<String> BUILT_IN_TYPES = Sets.newHashSet("Integer", "Long", "Boolean", "Float", "Double", "String", "Map", "List", "Set");

    private final Configuration cfg;

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        options.addOption(Option.builder().longOpt(OPT_SCHEMA_FILE_PATH).hasArg().required().desc("path to the schema file").build());
        options.addOption(Option.builder().longOpt(OPT_OUTPUT_PATH).hasArg().required().desc("output path").build());
        CommandLine cmd = parser.parse(options, args);

        //TODO: Test if this works
        if (options.getOptions().size() == 0 || options.hasOption("h") || options.hasLongOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CodeGenerator", options);
            System.exit(0);
        }
        ///

        String[] paths = cmd.getOptionValues(OPT_SCHEMA_FILE_PATH);
        CodeGenerator codeGenerator = new CodeGenerator();
        for (int idx = 0; idx < paths.length; idx++) {
            try (InputStream is = new FileInputStream(paths[idx])) {
                codeGenerator.generateCode(cmd.getOptionValue(OPT_OUTPUT_PATH), is);
            } catch (Exception e) {
                throw new RuntimeException("Error processing " + paths[idx], e);
            }
        }
    }

    public CodeGenerator() {
        cfg = new Configuration(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS);
        cfg.setClassForTemplateLoading(this.getClass(), "/templates");
        cfg.setLogTemplateExceptions(false);
    }

    public void generateCode(String outputPath, InputStream... schemaInputs) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        for (InputStream schemaInput : schemaInputs) {
            Schema schema = objectMapper.readValue(schemaInput, Schema.class);
            for (TableDefinition tableDefinition : schema.getTableDefinitions()) {
                generateSchemaClasses(tableDefinition, outputPath, cfg);
            }
        }
    }

    private void generateSchemaClasses(TableDefinition tableDefinition, String outputPath, Configuration cfg) throws Exception {
        Set<String> nonGeneratedCustomTypes = getNonGeneratedCustomTypes(tableDefinition);
        String packageDir = tableDefinition.getPackageName().replace(".", "/");
        FileUtils.forceMkdir(new File(outputPath + "/" + packageDir));

        Template interfaceTemplate = cfg.getTemplate("interface.ftl");
        Template beanTemplate = cfg.getTemplate("bean.ftl");
        Template updatesTemplate = cfg.getTemplate("updates.ftl");
        Template updateResultInterfaceTemplate = cfg.getTemplate("updateResultInterface.ftl");
        Template updateResultBeanTemplate = cfg.getTemplate("updateResultBean.ftl");
        Template updatesUpdateResultTemplate = cfg.getTemplate("updatesUpdateResult.ftl");
        Optional<Type> tableTypeOptional = tableDefinition.getTypes().stream().filter(t -> t.getName().equals(tableDefinition.getType())).findFirst();
        if (!tableTypeOptional.isPresent()) {
            throw new RuntimeException("Cannot find type definition for " + tableDefinition.getType());
        }
        Type tableType = tableTypeOptional.get();
        int typeSequence = 0;
        for (Type type : tableDefinition.getTypes()) {

            //Validate fields
            Set<String> uniqueFieldName = Sets.newHashSet();
            Set<String> uniqueFieldDynamoName = Sets.newHashSet();
            for (Field field : type.getFields()) {

                if (uniqueFieldName.contains(field.getName())) {
                    throw new IllegalArgumentException(String.format("Type: %s has a duplicated field name: %s. Field name must be unique.", type.getName(), field.getName()));
                }
                uniqueFieldName.add(field.getName());

                if (field.isPersist() && uniqueFieldDynamoName.contains(field.getDynamoName())) {
                    throw new IllegalArgumentException(String.format("Type: %s has a duplicated field dynamo name: %s. Field dynamo name must be unique.", type.getName(), field.getDynamoName()));
                }
                uniqueFieldDynamoName.add(field.getDynamoName());
            }

            Map<String, Object> model = new HashMap<>();
            String beanName = type.getName() + "Bean";
            String updatesName = type.getName() + "Updates";
            String updateResultName = type.getName() + "UpdateResult";
            String updateResultBeanName = type.getName() + "UpdateResultBean";
            String updatesUpdateResultName = updatesName + "UpdateResult";
            typeSequence++;
            model.put("typeSequence", typeSequence);
            model.put("tableDefinition", tableDefinition);
            model.put("package", tableDefinition.getPackageName());
            model.put("schemaVersion", tableDefinition.getVersion());
            model.put("schemaVersionField", tableDefinition.getSchemaVersionField());
            model.put("type", type);
            model.put("beanName", beanName);
            model.put("updatesName", updatesName);
            model.put("tableName", tableDefinition.getTableName());
            model.put("rootType", tableDefinition.getType());
            model.put("currentState", "current" + type.getName());
            Set<String> imports = new HashSet<>();
            for (Field field : type.getFields()) {
                if (nonGeneratedCustomTypes.contains(field.getElementType())) {
                    imports.add(field.getElementType());
                }
            }
            model.put("imports", imports);

            if (type.getName().equals(tableDefinition.getType())) {
                model.put("isRoot", true);
            } else {
                model.put("isRoot", false);
                Field field = tableType.getFields().stream().filter(f -> f.getElementType().equals(type.getName())).findFirst().get();
                model.put("parentFieldName", field.getDynamoName());
            }

            model.put("optimisticLocking", tableDefinition.isOptimisticLocking());
            model.put("revisionFieldName", Schema.REVISION_FIELD);
            model.put("schemaVersionFieldName", tableDefinition.getSchemaVersionField());

            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputPath + "/" + packageDir + "/" + type.getName() + ".java"))) {
                interfaceTemplate.process(model, writer);
            }
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputPath + "/" + packageDir + "/" + beanName + ".java"))) {
                beanTemplate.process(model, writer);
            }
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputPath + "/" + packageDir + "/" + updatesName + ".java"))) {
                updatesTemplate.process(model, writer);
            }
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputPath + "/" + packageDir + "/" + updateResultName + ".java"))) {
                updateResultInterfaceTemplate.process(model, writer);
            }
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputPath + "/" + packageDir + "/" + updateResultBeanName + ".java"))) {
                updateResultBeanTemplate.process(model, writer);
            }
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputPath + "/" + packageDir + "/" + updatesUpdateResultName + ".java"))) {
                updatesUpdateResultTemplate.process(model, writer);
            }

        }
    }

    private Set<String> getNonGeneratedCustomTypes(TableDefinition tableDefinition) {
        Set<String> customTypes = new HashSet<>();
        Set<String> generatedTypes = tableDefinition.getTypes().stream().map(t -> t.getName()).collect(Collectors.toSet());
        for (Type type : tableDefinition.getTypes()) {
            for (Field field : type.getFields()) {
                if (!generatedTypes.contains(field.getElementType()) && !BUILT_IN_TYPES.contains(field.getElementType())) {
                    customTypes.add(field.getElementType());
                }
            }
        }
        return customTypes;
    }

}
