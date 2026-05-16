package io.tabular.iceberg.connect;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;

/**
 * JSON-based configuration for flag messages. Configure via the {@code iceberg.flags.config}
 * connector property as a JSON object. All fields are free-form; use any key/value pairs needed.
 *
 * <p>Example:
 *
 * <pre>
 * iceberg.flags.config = {"field-name": "event", "custom-var": "value"}
 * </pre>
 */
public class FlagConfig {
    @JsonProperty("ddl-field-name")
    private String fieldName;

    @JsonProperty("ddl-key-flag")
    private String keyFlag;

    @JsonProperty("ddl-fields")
    private String fields;

    @JsonProperty("ddl-fields-modified")
    private String fieldsModified;

    @JsonProperty("ddl-type-default-value")
    private String typeValue;


    private final Map<String, String> additionalProperties = new java.util.LinkedHashMap<>();

    public FlagConfig() {}

    public FlagConfig(String fieldName, String keyFlag, String fields, String fieldsModified) {
        this.fieldName = fieldName;
        this.keyFlag = keyFlag;
        this.fields = fields;
        this.fieldsModified = fieldsModified;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getKeyFlag() {
        return keyFlag;
    }

    public String getFields() {
        return fields;
    }

    public String getFieldsModified() {
        return fieldsModified;
    }


    public String getTypeValue() {
        return typeValue;
    }

    /**
     * Any additional properties supplied in the JSON object that are not explicitly modelled above.
     * Useful for forward-compatibility without requiring a code change.
     */
    @JsonAnyGetter
    public Map<String, String> additionalProperties() {
        return Collections.unmodifiableMap(additionalProperties);
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, String value) {
        additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        Map<String, String> all = new java.util.LinkedHashMap<>();
        for (java.lang.reflect.Field f : getClass().getDeclaredFields()) {
            JsonProperty ann = f.getAnnotation(JsonProperty.class);
            if (ann == null) continue;
            f.setAccessible(true);
            try {
                Object val = f.get(this);
                if (val != null) all.put(ann.value(), val.toString());
            } catch (IllegalAccessException ignored) {}
        }
        all.putAll(additionalProperties);
        return "FlagConfig" + all;
    }
}