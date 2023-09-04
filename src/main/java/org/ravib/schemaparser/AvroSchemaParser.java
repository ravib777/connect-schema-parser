package org.ravib.schemaparser;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.data.SchemaAndValue;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AvroSchemaParser {
    public void parseSchema(String schemaFile,String messageFile,String configFile) {

        try {
            Schema schema = new Schema.Parser().parse(new File(schemaFile));
            String json = new String(Files.readAllBytes(Paths.get(messageFile)));
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
            DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
            GenericRecord record = reader.read(null, decoder);
            CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081",1000);
             schemaRegistryClient.register("avro-schema-value", new AvroSchema(schema));
            KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient);

            byte[] serializedRecord1 = serializer.serialize("avro-schema",record);

            AvroConverter converter = new AvroConverter();
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
            SchemaAndValue connectSchema = converter.toConnectData("avro-schema", serializedRecord1);
            System.out.println("Connect Schema is"+ connectSchema.toString());

        } catch(Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}
