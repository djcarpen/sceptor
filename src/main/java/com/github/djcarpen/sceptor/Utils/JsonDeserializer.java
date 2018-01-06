package com.github.djcarpen.sceptor.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JsonDeserializer {

    //private List<DataDictionary.HiveTable> sourceTables = new ArrayList<>();
    private DataDictionary dataDictionary = new DataDictionary();

    public DataDictionary getDataDictionary() {
        return dataDictionary;
    }

    public DataDictionary generateSourceSchema(String jsonPath) throws IOException {
            Files.walk(Paths.get(jsonPath)).filter(path -> path.toString().endsWith(".json")).forEach((Path path) -> {
                System.out.println(path);
                ObjectMapper objectMapper = new ObjectMapper();

                HiveTable sourceTable;
                try {
                    sourceTable = objectMapper.readValue(new File(path.toString()), HiveTable.class);
                    dataDictionary.addTable(sourceTable);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        return dataDictionary;
    }

}
