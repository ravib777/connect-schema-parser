package org.ravib.schemaparser;

public class SchemaParser {
    private static String schemaType=null;
    private static String schemaFile=null;
    private static String messageFile=null;
    private static String configFile=null;

    public static void main(String args []){
        if (args.length < 3) {
            System.err.println("Usage: java org.ravib.SchemaParser <schemaType> <schemaFile> <messageFile> <optional_Config_File>");
            System.err.println("Usage: schemaType needs to be one of avro, protobuf, jsonschema");
            System.exit(1);
        }
        else {
            schemaType = args[0];
            schemaFile = args[1];
            messageFile = args[2];
            if(args.length == 4 )
                configFile = args[3];
        }

        switch (args[0]) {
            case "avro":
                AvroSchemaParser avroParser = new AvroSchemaParser();
                avroParser.parseSchema(schemaFile,messageFile,configFile);
                break;
            case "jsonschema":
                JsonSchemaParser jsonSchemaParser = new JsonSchemaParser();
                jsonSchemaParser.parseSchema(schemaFile,messageFile,configFile);
                break;
            case "protobuf":
                ProtobufParser protoParser = new ProtobufParser();
                protoParser.parseSchema(schemaFile,messageFile,configFile);
                break;
            default:
                System.err.println("Invalid argument: " + args[0]);
                System.exit(1);
        }
    }
}
