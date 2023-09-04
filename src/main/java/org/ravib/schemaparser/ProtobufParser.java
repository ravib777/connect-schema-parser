package org.ravib.schemaparser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.*;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProtobufParser {

    public void parseSchema(String schemaFile,String messageFile,String configFile) {
        try {
            String jsonMessagePath = messageFile;
            String jsonMessageString = new String(Files.readAllBytes(Paths.get(jsonMessagePath)));

            String jsonSchemaPath = schemaFile;
            String jsonSchemaString = new String(Files.readAllBytes(Paths.get(jsonSchemaPath)));

            SchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
            ParsedSchema parsedSchema = protobufSchemaProvider.parseSchemaOrElseThrow(
                    new Schema(null, null, null, ProtobufSchema.TYPE, new ArrayList<>(), jsonSchemaString), false, false);
            Optional<ParsedSchema> parsedSchemaOptional = protobufSchemaProvider.parseSchema(jsonSchemaString,
                    new ArrayList<>(), false, false);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(jsonMessageString);
            Object Objectmessage =  ProtobufSchemaUtils.toObject(jsonNode, (ProtobufSchema) parsedSchema);
            CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081", 1000);
            schemaRegistryClient.register("proto-subject-value",parsedSchema);
            KafkaProtobufSerializer serializer = new KafkaProtobufSerializer(schemaRegistryClient);
            byte[] serializedRecord1 = serializer.serialize("proto-subject", (Message) Objectmessage);
            ProtobufConverter converter = new ProtobufConverter();
            Map<String, String> configMap = new HashMap<>();
            configMap.put("schema.registry.url", "http://localhost:8081");
            if(configFile!=null){
                try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty() && !line.startsWith("#")) {
                            String[] parts = line.split("=", 2);
                            if (parts.length == 2) {
                                String key = parts[0].trim();
                                String value = parts[1].trim();
                                configMap.put(key, value);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            converter.configure(configMap, false);
            SchemaAndValue connectSchema =converter.toConnectData("proto-subject", serializedRecord1);
            System.out.println("Connect Schema is"+ connectSchema.toString());

        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}

