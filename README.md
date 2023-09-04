# SCHEMA VALIDATION UTILITY
This utility is intended to parse the schema and validate it against Kafka Connect schema.

### BUILDING THE PROJECT
To build the project, run the following command in the project directory:

`mvn clean package`

This will compile the project and create a JAR file in the `target` directory.

### Running the Project

To run the project, use the following command:

`mvn exec:java  -Dexec.mainClass=org.ravib.schemaparser.SchemaParser -Dexec.args="<schemaType> <schemaFile> <messageFile> <converterConfigFile>"`

#### Arguments
* `schemaType`: Needs to be one of avro, protobuf or jsonschema
* `schemaFile`: Path of the schema file. For example, point to a avsc file in case of avro or a .proto file for protobuf
* `messageFile`: Path of the message file. The message needs to be in json format
* `converterConfigFile` (optional): By default the utility connects to http://localhost:8081 schema registry without any credentials. This config file can be used to override the SR location. Also, SR aware Converters support multiple options documented here https://docs.confluent.io/platform/current/schema-registry/connect.html#example-converter-properties. Use this config file to set the converter configs.

##### Tip
Set `kafka.client.tag` and `kafka.tag` based on customer's CP/AK version in `pom.xml`

