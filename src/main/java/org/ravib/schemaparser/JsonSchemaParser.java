package org.ravib.schemaparser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.*;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.connect.json.*;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class JsonSchemaParser {
    public void parseSchema(String schemaFile,String messageFile,String configFile) {
        JsonSchemaParser jsonSchemaParser = new JsonSchemaParser();

        try {
            JsonNode rawSchemaJson = jsonSchemaParser.readJsonNode(schemaFile);

            ObjectMapper mapper = new ObjectMapper();
            File from = new File(messageFile);
            JsonNode masterJSON = mapper.readTree(from);

            CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081",1000);
             schemaRegistryClient.register("json-schema-value", new JsonSchema(rawSchemaJson));

            KafkaJsonSchemaSerializer serializer = new KafkaJsonSchemaSerializer(schemaRegistryClient);

            byte[] serializedRecord1 = serializer.serialize("json-schema",
                    JsonSchemaUtils.envelope(rawSchemaJson, masterJSON)
            );

            JsonSchemaConverter converter = new JsonSchemaConverter();
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
            SchemaAndValue connectSchema = converter.toConnectData("json-schema", serializedRecord1);
            System.out.println("Connect Schema is" + connectSchema);

        } catch(Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

    public JsonNode readJsonNode(String relPath) throws IOException {
        try (InputStream stream = getStream(relPath)) {
            return new ObjectMapper().readTree(stream);
        }
    }
    public InputStream getStream(String relPath) throws IOException {
        String absPath = relPath;
        File initialFile = new File(absPath);
        InputStream inputStream = new FileInputStream(initialFile);
        return inputStream;
    }
}
