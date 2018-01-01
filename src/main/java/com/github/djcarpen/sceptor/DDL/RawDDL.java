package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Schema.SourceSchema;
import com.github.djcarpen.sceptor.Zone;

public class RawDDL implements DDL {


    SourceSchema tableMapper;

    SourceSchema stagingTable = new SourceSchema();







    public void writeDDL(Zone zone, Schema schema, String ddlPath) {

    }
}
