package com.github.djcarpen.sceptor.DML;

import com.github.djcarpen.sceptor.Schema.DataVault.HubSchema;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class BDWDML {
    private Properties Props = new Properties();

    private static final String hubKeyDelimiter = "-";
    private static final String datePattern = "MM/dd/yyyy HH:mm:ss";
    private static final DateFormat df = new SimpleDateFormat(datePattern);
    private static final Date dateobj = new Date();
    private static final String load_dt = df.format(dateobj);

    private Map<String, String> dmlMap;
    private Map<String, String> dmlMapHubs;
    private Map<String, String> dmlMapSatellites;
    private Map<String, String> dmlMapLinks;

    public BDWDML() {
        try {
            Props.load(new FileInputStream("Runtime.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map getDMLs(List<Schema> schemas) {


        dmlMap = new LinkedHashMap<>();
        dmlMap.putAll(getHubDMLS(schemas.get(0)));
        dmlMap.putAll(getSatelliteDMLS(schemas.get(1)));
        dmlMap.putAll(getLinkDMLs(schemas.get(2)));
        return dmlMap;
    }


    public Map getHubDMLS(Schema schema) {
        dmlMapHubs = new LinkedHashMap<>();
        for (HubSchema.HubTable h : ((HubSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();

            dmlMapHubs.put("load_" + h.getTableName() + ".hql", sb.toString());
        }
        return dmlMapHubs;
    }


    public Map getSatelliteDMLS(Schema schema) {
        dmlMapSatellites = new LinkedHashMap<>();

        return dmlMapSatellites;
    }

    public Map getLinkDMLs(Schema schema) {
        dmlMapLinks = new LinkedHashMap<>();

        return dmlMapLinks;
    }
}
