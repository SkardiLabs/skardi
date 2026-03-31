use uuid::Uuid;

pub struct RedisRelation;

impl RedisRelation {
    /// Builds the key prefix: `{keyspace}:{table_name}` or just `{table_name}` if keyspace is empty.
    pub fn prefix(keyspace: &str, table_name: &str) -> String {
        if keyspace.is_empty() {
            table_name.to_string()
        } else {
            format!("{}:{}", keyspace, table_name)
        }
    }

    /// Returns the schema key: `{keyspace}:{table_name}:schema` (or `{table_name}:schema`).
    pub fn schema_key(keyspace: &str, table_name: &str) -> String {
        format!("{}:schema", Self::prefix(keyspace, table_name))
    }

    /// Generates a new random UUID (v4) as a hyphen-free lowercase string.
    pub fn uuid() -> String {
        Uuid::new_v4()
            .as_simple()
            .encode_lower(&mut Uuid::encode_buffer())
            .to_string()
    }

    /// Returns a data key: `{prefix}:{id}`.
    /// If `id` is None, generates a new UUID without hyphens.
    pub fn data_key(keyspace: &str, table_name: &str, id: Option<&str>) -> String {
        let id_str = match id {
            Some(s) => s.to_string(),
            None => Self::uuid(),
        };
        format!("{}:{}", Self::prefix(keyspace, table_name), id_str)
    }

    /// Returns the pattern matching all data keys for a table: `{prefix}:*`
    pub fn table_data_key_pattern(keyspace: &str, table_name: &str) -> String {
        format!("{}:*", Self::prefix(keyspace, table_name))
    }

    /// Given a keys-prefix-pattern and a full redis key,
    /// strip off the prefix if the pattern ends with `*`,
    /// otherwise return the key unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use skardi_sources::providers::redis::relation::RedisRelation;
    ///
    /// let pattern = "users:*";
    /// let key = "users:1234";
    /// assert_eq!(RedisRelation::table_key(pattern, key), "1234");
    ///
    /// let pattern2 = "users:";
    /// assert_eq!(RedisRelation::table_key(pattern2, key), "users:1234");
    /// ```
    pub fn table_key<'a>(key_prefix_pattern: &'a str, redis_key: &'a str) -> &'a str {
        if key_prefix_pattern.ends_with('*') {
            let prefix_len = key_prefix_pattern.len() - 1;
            &redis_key[prefix_len..]
        } else {
            redis_key
        }
    }
}
