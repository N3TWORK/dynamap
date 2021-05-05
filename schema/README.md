# Dynamap JSON Schema

This is a JSON-schema for Dynamap JSON schema definitions. The schema is hosted at:

http://schema.n3twork.com/dynamap/v1/schema.json

The easiest way to make use of this schema is to reference it at the top of your Dynamap 
JSON documents:

```
"$schema": "https://schema.n3twork.com/dynamap/v1/schema.json",
```

Most modern editors (VS Code/IDEA) will load the schema and will provide editor hints as you author your 
documents so you know you're writing a valid schema.

You can also validate a schema locally using the `validate` command:

```
validate -f my-dynamap-schema.json
```

The `validate` command uses `ajv`, which you can install locally with npm:
```
npm install -g ajv
```