package com.github.djcarpen.sceptor.Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

class FileWriter {

    public void writeFile(String fileContents, String outputPath, String fileName) {
        File file = new File(outputPath + "/" + fileName);
        if (!fileContents.isEmpty()) {
            try {
                java.io.FileWriter fw = new java.io.FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(fileContents);
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

    }
}
