package com.github.djcarpen.sceptor.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.djcarpen.sceptor.Schema.DataDictionary;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JsonDeserializer {

    private final DataDictionary dataDictionary = new DataDictionary();

    public DataDictionary generateDataDictionary(String jsonPath) throws IOException {
        Files.walk(Paths.get(jsonPath)).filter(path -> path.toString().endsWith(".json")).forEach((Path path) -> {
            //System.out.println(path);
            ObjectMapper objectMapper = new ObjectMapper();
            DataDictionary.Table sourceTable;
            try {
                sourceTable = objectMapper.readValue(new File(path.toString()), DataDictionary.Table.class);
                dataDictionary.addTable(sourceTable);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return dataDictionary;
    }

}
