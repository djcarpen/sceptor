package com.github.djcarpen.sceptor;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyHandler {
    private static PropertyHandler instance = null;

    private Properties props = null;

    private PropertyHandler() {

        this.props = new Properties();
        try {
            props.load(new FileInputStream("application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized PropertyHandler getInstance() {
        if (instance == null)
            instance = new PropertyHandler();
        return instance;
    }

    public String getValue(String propKey) {
        return this.props.getProperty(propKey);
    }
}
