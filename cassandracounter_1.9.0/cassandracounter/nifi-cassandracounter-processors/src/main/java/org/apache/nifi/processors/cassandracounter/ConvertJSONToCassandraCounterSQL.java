/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.cassandracounter;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.*;

@Tags({"json","Canssandra Counter table","Update Statement"})
@CapabilityDescription("Converts a Json Flowfile to cassandra Counter table Update Statment ")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute="mime.type", description="Sets mime.type of FlowFile that is routed to 'sql' to 'text/plain'."),
        @WritesAttribute(attribute = "<sql>.table", description = "Sets the <sql>.table attribute of FlowFile that is routed to 'sql' to the name of the table that is updated by the SQL statement. "
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute="<sql>.catalog", description="If the Catalog name is set for this database, specifies the name of the catalog that the SQL statement will update. "
                + "If no catalog is used, this attribute will not be added. The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute="fragment.identifier", description="All FlowFiles routed to the 'sql' relationship for the same incoming FlowFile (multiple will be output for the same incoming "
                + "FlowFile if the incoming FlowFile is a JSON Array) will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute="fragment.count", description="The number of SQL FlowFiles that were produced for same incoming FlowFile. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming FlowFile."),
        @WritesAttribute(attribute="fragment.index", description="The position of this FlowFile in the list of outgoing FlowFiles that were all derived from the same incoming FlowFile. This can be "
                + "used in conjunction with the fragment.identifier and fragment.count attributes to know which FlowFiles originated from the same incoming FlowFile and in what order the SQL "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute="<sql>.args.N.type", description="The output SQL statements are parametrized in order to avoid SQL Injection Attacks. The types of the Parameters "
                + "to use are stored in attributes named <sql>.args.1.type, <sql>.args.2.type, <sql>.args.3.type, and so on. The type is a number representing a JDBC Type constant. "
                + "Generally, this is useful only for software to read and interpret but is added so that a processor such as PutSQL can understand how to interpret the values. "
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute="<sql>.args.N.value", description="The output SQL statements are parametrized in order to avoid SQL Injection Attacks. The values of the Parameters "
                + "to use are stored in the attributes named sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. Each of these attributes has a corresponding "
                + "<sql>.args.N.type attribute that indicates how the value should be interpreted when inserting it into the database."
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property.")
})
public class ConvertJSONToCassandraCounterSQL extends AbstractProcessor implements Processor {

    private static final String UPDATE_TYPE = "UPDATE";
    private static final String ADDITION="+";
    private static final String SUBTRACTION="-";

    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched columns", "Ignore Unmatched columns",
            "Any Column in the database that does not have a field in the JSON Document will be assumed as not important ");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns", "Fail on Unmatched Columns",
            "A Flow file will fail if a column in the JSON property is not part of the database columns");

    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "Any field in the JSON that does not have a column in the database document will be assumed to not be required.  A warning will be logged");

    static final PropertyDescriptor SQL_PARAM_ATTR_PREFIX = new PropertyDescriptor.Builder()
            .name("jts-sql-param-attr-prefix")
            .displayName("SQL Parameter Attribute Prefix")
            .description("The string to be prepended to the outgoing flow file attributes, such as <sql>.args.1.value, where <sql> is replaced with the specified value")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .required(true)
            .defaultValue("sql")
            .build();

    public static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor
            .Builder().name("Statement type")
            .displayName("Statement type")
            .description("Specifies the type of SQL statement to generate UPSERT ")
            .required(true)
            .allowableValues(UPDATE_TYPE)
            .build();
    //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    //.build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("Table name")
            .displayName("Cassandra Table name")
            .description("Specifies the table name to run the UPSERT statement")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("Unmatched Field Behavior")
            .description("If an incoming JSON element has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(IGNORE_UNMATCHED_COLUMN.getValue())
            .build();

    public static final PropertyDescriptor KEY_SPACE = new PropertyDescriptor
            .Builder().name("Keyspace name")
            .displayName("Cassandra Keyspace name")
            .description("Specifies the Keyspace name to run the UPSERT statement, Does not support Expression language")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("Translate Field Names")
            .description("If true, the Processor will attempt to translate JSON field names into the appropriate column names for the table specified. "
                    + "If false, the JSON field names must match the column names exactly, or the column will not be updated")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor UPDATE_KEY = new PropertyDescriptor.Builder()
            .name("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPSERT statements. "
                    + "If the Statement Type is UPDATE and this property is not set, the table's Primary Keys are used. "
                    + "In this case, if no Primary Key exists, the conversion to SQL will fail if Unmatched Column Behaviour is set to FAIL. "
                    + "This property is ignored if the Statement Type is INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor COUNTER_COLUMNS = new PropertyDescriptor.Builder()
            .name("Counter Columns Keys")
            .description("A comma-separated list of column names that form part of the counter columns.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor COUNTER_OPERATOR = new PropertyDescriptor.Builder()
            .name("Operator")
            .description("Operrator to use for the counter e.g +/-")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(ADDITION,SUBTRACTION)
            .required(true)
            .build();

    static final PropertyDescriptor TABLE_SCHEMA_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("table-schema-cache-size")
            .displayName("Table Schema Cache Size")
            .description("Specifies how many Table Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor QUOTED_IDENTIFIERS = new PropertyDescriptor.Builder()
            .name("jts-quoted-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to "
                    + "use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SQL = new Relationship.Builder()
            .name("sql")
            .description("files that were successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("files that were not successfully processed")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("When a FlowFile is converted to SQL, the original JSON FlowFile is routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;

    private Set<Relationship> relationships;
    //private Object InputStreamCallback;
    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> descriptors = getSupportedPropertyDescriptors();
        this.properties = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SQL);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }


    @Override
    protected final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(STATEMENT_TYPE);
        descriptors.add(UPDATE_KEY);
        descriptors.add(TABLE_NAME);
        descriptors.add(KEY_SPACE);
        descriptors.add(TABLE_SCHEMA_CACHE_SIZE);
        descriptors.add(TRANSLATE_FIELD_NAMES);
        descriptors.add(COUNTER_COLUMNS);
        descriptors.add(COUNTER_OPERATOR);
        descriptors.add(QUOTED_IDENTIFIERS);
        descriptors.add(UNMATCHED_FIELD_BEHAVIOR);
        descriptors.add(SQL_PARAM_ATTR_PREFIX);
        return descriptors;
    }



    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        /*final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                .maximumSize(tableSchemaCacheSize)
                .build();*/

    }

    private Set<String> getJsonFieldNames(final JsonNode node) {
        final Set<String> JsonFieldNames = new HashSet<>();
        final Iterator<String> filedNamesItr = node.fieldNames();
        while (filedNamesItr.hasNext()) {
            JsonFieldNames.add(filedNamesItr.next());
        }
        return JsonFieldNames;
    }

    private static String normalizedColumnNames(final String colName, final Boolean translateColumnNames) {
        return translateColumnNames ? colName.replace("_", "") : colName;

    }

    private String generateUpdate(final JsonNode jsonNode, final String statementType, final Map<String, String> attributes, final String keySpace
            , final String tableName, final String updateKeys, final String counterColumns, final String operator, final Boolean failUnmappedColumns, final Boolean warningUmappedColumns,
                                  String attributePrefix) {
        final String counterColsDataType = "DOUBLE";
        final String updateKeysDataType = "TEXT";
        final Set<String> updateKeysNames;
        final Set<String> counterColumnskeys;
        if (updateKeys != null) {
            updateKeysNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeysNames.add(updateKey.trim());
            }
        } else {
            // Throw Error
            //TODO implement
            throw new ProcessException(" No Update Keys were Specified! Please Specify Destinations Primary key Columns and retry ");
        }
        if (counterColumns != null) {
            counterColumnskeys = new HashSet<>();
            for (final String counterColKeys : counterColumns.split(",")) {
                counterColumnskeys.add(counterColKeys);
            }
        } else {
            // Throw Error
            //TODO implement
            throw new ProcessException(" No Counter Columns Specified! Please Specify Counter columns and retry ");
        }
        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(keySpace + "." + tableName);
        sqlBuilder.append(" SET ");

        //Compare input UpdateKeys and to confirm they exist in the incoming JSON Document
        final Set<String> jsonFieldNames = getJsonFieldNames(jsonNode);
        for (final String upK : updateKeysNames) {
            if (!jsonFieldNames.contains(upK)) {
                String missingColMsg = "JSON does not have value for " + (updateKeys == null ? "Primary " : " Update ") + " Key Column '" + upK + "'";
                if (failUnmappedColumns) {
                    getLogger().error(missingColMsg);
                    throw new ProcessException(missingColMsg);
                } else if (warningUmappedColumns) {
                    getLogger().warn(missingColMsg);
                }
            }
        }

        for (final String counterCols : counterColumnskeys) {
            if (!jsonFieldNames.contains(counterCols)) {
                String missingColMsg = "JSON does not have value for " + (counterColumns == null ? "Primary " : " Update ") + " Key Column '" + counterCols + "'";
                if (failUnmappedColumns) {
                    getLogger().error(missingColMsg);
                    throw new ProcessException(missingColMsg);
                } else if (warningUmappedColumns) {
                    getLogger().warn(missingColMsg);
                }
            }
        }

        // Iterate over all the JSON elements, Building the SQL Statement by adding the column names as well as the values
        //Add the column values to a "<sql>.args.N.value" attribute and the type of a "<sql>.args.N.type" attribute add the
        //columns that we are inserting into

        final Set<String> counterjsonFdNames = getJsonFieldNames(jsonNode);
        //Iterator<String> fieldNames = jsonNode.fieldNames();
        Iterator<String> cKeys=counterColumnskeys.iterator();
        while (cKeys.hasNext()) {
            final String fieldName = cKeys.next();
            if (counterjsonFdNames.contains(fieldName)) {
                sqlBuilder.append(fieldName);
                if (fieldCount++ > 0) {
                    sqlBuilder.append(" ,");
                }
                sqlBuilder.append(" = " + fieldName + " " + operator + " ? ");
                attributes.put(attributePrefix + ".args." + fieldCount + ".value", counterColsDataType);
                final JsonNode fieldNode = jsonNode.get(fieldName);
                if (!fieldNode.isNull()) {
                    String fieldValue = fieldNode.asText();
                    attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
                }
            }

        }
        sqlBuilder.append(" WHERE ");
        int whereFieldCount = 0;
        final Set<String> updatejsonFdNames = getJsonFieldNames(jsonNode);
        Iterator<String> upKeys =updateKeysNames.iterator();
        //fieldNames = jsonNode.fieldNames();
        while (upKeys.hasNext()) {
            final String uFieldName = upKeys.next();
            if (whereFieldCount++ > 0) {
                sqlBuilder.append(" AND ");
            }

            fieldCount++;
            if (updatejsonFdNames.contains(uFieldName)) {
                sqlBuilder.append(uFieldName);
                sqlBuilder.append(" = ? ");
                attributes.put(attributePrefix + ".args." + fieldCount + ".value", updateKeysDataType);
                final JsonNode fieldNode = jsonNode.get(uFieldName);
                if (!fieldNode.isNull()) {
                    String fieldValue = fieldNode.asText();
                    attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
                }
            }
        }
        sqlBuilder.append(";");
        return sqlBuilder.toString();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        // TODO implement
        // Getting the user input  properties into variable
        final String statemenType = context.getProperty(STATEMENT_TYPE).getValue();
        final String counterColumns = context.getProperty(COUNTER_COLUMNS).evaluateAttributeExpressions(flowFile).getValue();
        final String updateKey = context.getProperty(UPDATE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String operator = context.getProperty(COUNTER_OPERATOR).evaluateAttributeExpressions(flowFile).getValue();
        final String keySpace = context.getProperty(KEY_SPACE).getValue();
        final Boolean includePrimaryKeys = UPDATE_TYPE.equals(statemenType) && updateKey != null;

        // If the unmatched column behaviour fail or warning
        final Boolean failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());
        final Boolean warningUmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());
        // Attribute prefix
        final String attributePrefix = context.getProperty(SQL_PARAM_ATTR_PREFIX).evaluateAttributeExpressions(flowFile).getValue();


        // Parse JSON Document
        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> jsonNodeRef = new AtomicReference<>(null);
        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream In) throws IOException {
                    try (final InputStream bufferedIn = new BufferedInputStream(In)) {
                        jsonNodeRef.set(mapper.readTree(bufferedIn));
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to pass {} as a JSON due to {}; routing to failure", new Object[]{flowFile, pe.toString()}, pe);
            session.transfer(flowFile, REL_FAILURE);
        }

        // Convert the Flowfile to Array for easy Iteration
        final JsonNode jsonNodeRoot = jsonNodeRef.get();
        final ArrayNode arrayNode;

        if (jsonNodeRoot.isArray()) {
            arrayNode = (ArrayNode) jsonNodeRoot;
        } else {
            final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
            arrayNode = new ArrayNode(nodeFactory);
            arrayNode.add(jsonNodeRoot);

        }

        final String fragmentIdentifier = UUID.randomUUID().toString();
        final HashSet<FlowFile> created = new HashSet<>();
        for (int i = 0; i < arrayNode.size(); i++) {
            final JsonNode jsonNode = arrayNode.get(i);
            final String sql;
            final Map<String, String> attributes = new HashMap<>();
            try {
                if (UPDATE_TYPE.equalsIgnoreCase(statemenType)) {
                    sql = generateUpdate(jsonNode, statemenType, attributes, keySpace, tableName, updateKey, counterColumns,operator, failUnmappedColumns, warningUmappedColumns, attributePrefix);
                }else{
                    sql=null;
                }
            } catch (final ProcessException pe) {
                getLogger().error("Failed to convert SQL {} statement due to {}; routing to failure ", new Object[]{flowFile, statemenType, pe.toString()}, pe);
                session.remove(created);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            FlowFile sqlFlowfile = session.create(flowFile);
            created.add(sqlFlowfile);
            sqlFlowfile = session.write(sqlFlowfile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream outputStream) throws IOException {
                    if (sql!=null) {
                        outputStream.write(sql.getBytes(StandardCharsets.UTF_8));
                    }
                }
            });
            attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
            attributes.put(attributePrefix + ".table", tableName);
            attributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
            attributes.put(FRAGMENT_COUNT.key(), String.valueOf(arrayNode.size()));
            attributes.put(FRAGMENT_INDEX.key(), String.valueOf(i));
            sqlFlowfile = session.putAllAttributes(sqlFlowfile, attributes);
            session.transfer(sqlFlowfile, REL_SQL);
        }

        FlowFile newFlowFile = copyAttributesToOriginal(session,flowFile,fragmentIdentifier,arrayNode.size());
        session.transfer(newFlowFile,REL_ORIGINAL);
    }

}