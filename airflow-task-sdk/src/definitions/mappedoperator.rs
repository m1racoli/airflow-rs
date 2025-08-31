use airflow_common::serialization::serde::JsonValue;

/// Whether a value can be used for task mapping.
pub fn is_mappable_value(value: &JsonValue) -> bool {
    matches!(value, JsonValue::Object(_) | JsonValue::Array(_))
}
