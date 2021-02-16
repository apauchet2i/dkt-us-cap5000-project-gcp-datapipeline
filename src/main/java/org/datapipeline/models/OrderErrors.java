package org.datapipeline.models;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.sql.PreparedStatement;
import java.util.*;

public class OrderErrors {

    public static TableSchema getTableSchemaOrderErrors() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("order_number").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("error_type").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("DATETIME").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("source").setType("STRING").setMode("NULLABLE"));
        return new TableSchema().setFields(fields);
    }
    public static void setParametersOrderErrorsSQL(TableRow element, PreparedStatement preparedStatement) throws Exception {
        preparedStatement.setString(1, element.get("order_number").toString());
        preparedStatement.setString(2, element.get("error_type").toString());
        preparedStatement.setString(3, element.get("updated_at").toString());
        preparedStatement.setString(4, element.get("source").toString());
    }
}
