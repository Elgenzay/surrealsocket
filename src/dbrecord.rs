use crate::error::SurrealSocketError;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};
use serde::{Deserialize, Deserializer, Serializer};
use std::marker::PhantomData;
use std::{
    any::Any,
    collections::HashMap,
    fmt::{Display, Formatter},
};
use surrealdb::engine::remote::ws::Client;
use surrealdb::sql::Thing;
use surrealdb::sql::{Id, Uuid};
use surrealdb::Surreal;

/// Methods associated with SurrealDB tables
///
/// This trait should be implemented for concrete types.
/// For generic types, use the `GenericResource` struct instead.
#[async_trait]
pub trait DBRecord: Any + Serialize + DeserializeOwned + Send + Sync {
    /// Get the associated table name
    fn table() -> &'static str;

    /// Get the UUID associated with the record
    fn uuid(&self) -> SsUuid<Self>;

    /// Get the Thing (`surrealdb::sql::thing::Thing`) associated with the record
    fn thing(&self) -> Thing {
        self.uuid().thing()
    }

    /// Whether records should be moved to a table named `z_trashed_{table}` on `db_delete()`
    fn use_trash() -> bool {
        false
    }

    /// This method is called immediately before a record is deleted by `db_delete()`.
    ///
    /// Override this method to perform checks or cleanup tasks before the object's deletion.
    ///
    /// If the method returns an `Error`, the deletion will be aborted and `db_delete()` will return the error.
    async fn delete_hook(&self) -> Result<(), SurrealSocketError> {
        Ok(())
    }

    /// Get an object from SurrealDB by its ID, or `None` if not found.
    ///
    /// Returns an `Error` if SurrealDB unexpectedly fails.
    async fn db_by_id(
        client: &Surreal<Client>,
        id: &str,
    ) -> Result<Option<Self>, SurrealSocketError> {
        let thing = Thing::from((Self::table().to_owned(), id.to_owned()));
        let uuid: SsUuid<Self> = SsUuid::from(thing);
        let item: Option<Self> = Self::db_search_one(client, "uuid", uuid).await?;
        Ok(item.into_iter().next())
    }

    /// Get a `Vec` of objects in the database where `field` matches `value`.
    ///
    /// Returns an `Error` if SurrealDB unexpectedly fails.
    ///
    /// If only one record at most is expected, use search_one() for an `Option` instead of a `Vec`.
    async fn db_search<T: Serialize + Clone + Send + 'static>(
        client: &Surreal<Client>,
        field: &str,
        value: T,
    ) -> Result<Vec<Self>, SurrealSocketError> {
        Self::db_query(client, SQLCommand::Select, field.to_string(), '=', value).await
    }

    async fn db_query<T: Serialize + Clone + Send + 'static>(
        client: &Surreal<Client>,
        sql_command: SQLCommand,
        field: String,
        operand: char,
        value: T,
    ) -> Result<Vec<Self>, SurrealSocketError> {
        client.set("table", Self::table()).await?;
        client.set("value", value).await?;

        // Update when this issue is resolved:
        // https://github.com/surrealdb/surrealdb/issues/1693
        let query = format!(
            "{} FROM type::table($table) WHERE {} {} $value",
            sql_command, field, operand
        );

        let mut response = client.query(query).await?;
        let result: Vec<Self> = response.take(0)?;
        Ok(result)
    }

    /// Get a single object in the database where `field` matches `value`, or `None` if not found.
    ///
    /// Returns an `Error` if SurrealDB unexpectedly fails.
    ///
    /// If searching by `id`, use `from_id()` instead.
    async fn db_search_one<T: Serialize + Clone + Send + 'static>(
        client: &Surreal<Client>,
        field: &str,
        value: T,
    ) -> Result<Option<Self>, SurrealSocketError> {
        Ok(Self::db_search(client, field, value)
            .await?
            .into_iter()
            .next())
    }

    /// Add a new record to the database and return it.
    async fn db_create(&self, client: &Surreal<Client>) -> Result<Self, SurrealSocketError> {
        let serde_value = serde_json::to_value(self)?;
        let id = self.uuid().uuid_string();

        let opt: Option<Self> = client
            .create((Self::table(), id.to_owned()))
            .content(serde_value)
            .await?;

        match opt {
            Some(e) => Ok(e),
            None => Err(SurrealSocketError::new("Failed to create record")),
        }
    }

    /// Delete a record from the database.
    async fn db_delete(&self, client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        self.delete_hook().await?;

        if Self::use_trash() {
            let created: Option<Self> = client
                .create((
                    format!("z_trashed_{}", Self::table()),
                    self.uuid().uuid_string(),
                ))
                .content(serde_json::to_value(self)?)
                .await?;

            created.ok_or_else(|| {
                SurrealSocketError::new(&format!(
                    "Failed to create trash table record: {}",
                    self.uuid().uuid_string()
                ))
            })?;
        }

        let thing = self.thing();
        let _: Option<Self> = client.delete((thing.tb, self.uuid().uuid_string())).await?;
        Ok(())
    }

    /// Update a single field of a record in the database.
    ///
    /// Use `db_update_fields()` to update multiple fields at once.
    async fn db_update_field<T: Serialize + Sync>(
        &self,
        client: &Surreal<Client>,
        field: &str,
        value: &T,
    ) -> Result<(), SurrealSocketError> {
        self.db_update_fields(client, vec![(field, value)]).await?;
        Ok(())
    }

    /// Update several fields of a record in the database at once.
    ///
    /// The first value of the tuple is the field name, and the second is the value to set.
    async fn db_update_fields<T: Serialize + Sync + Send + Clone>(
        &self,
        client: &Surreal<Client>,
        updates: Vec<(&str, T)>,
    ) -> Result<(), SurrealSocketError> {
        let mut merge_data = HashMap::<String, serde_json::Value>::new();
        merge_data.insert("updated_at".to_owned(), serde_json::to_value(Utc::now())?);

        for update in updates {
            merge_data.insert(update.0.to_owned(), serde_json::to_value(update.1.clone())?);
        }

        let _: Option<Self> = client
            .update((Self::table(), self.uuid().uuid_string()))
            .merge(merge_data)
            .await?;

        Ok(())
    }

    async fn db_all(client: &Surreal<Client>) -> Result<Vec<Self>, SurrealSocketError> {
        let table = Self::table();
        client.set("table", table).await?;
        let mut response = client.query("SELECT * FROM type::table($table)").await?;
        let result: surrealdb::Value = response.take(0)?;
        let serde_value = result.into_inner().into_json();
        let value: Vec<Self> = serde_json::from_value(serde_value).unwrap();
        Ok(value)
    }

    /// For each record in the table, add any missing properties with default values to the record in the database.
    ///
    /// Record retrieval already uses default values for missing fields, but this exists just in case it's ever needed.
    #[allow(unused)]
    async fn db_populate_defaults(client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        let result = Self::db_all(client).await?;
        let table = Self::table();

        for item in result {
            let _: Option<Self> = client
                .update((table, item.uuid().uuid_string()))
                .content(item)
                .await?;
        }

        Ok(())
    }

    async fn db_delete_table(client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        let table = Self::table();
        client.set("table", table).await?;
        client.query(format!("REMOVE TABLE {}", table)).await?;
        Ok(())
    }
}

pub enum SQLCommand {
    Select,
    Delete,
}

impl Display for SQLCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SQLCommand::Select => write!(f, "SELECT *"),
            SQLCommand::Delete => write!(f, "DELETE"),
        }
    }
}

/// A typed wrapper for the `Thing` object that corresponds to an ID in Surreal.
#[derive(Debug)]
pub struct SsUuid<T>(Thing, PhantomData<T>);

impl<T: DBRecord> Clone for SsUuid<T> {
    fn clone(&self) -> Self {
        Self(self.0.to_owned(), PhantomData)
    }
}

impl<T: DBRecord> From<Thing> for SsUuid<T> {
    fn from(thing: Thing) -> Self {
        SsUuid(thing, PhantomData)
    }
}

impl<T> SsUuid<T>
where
    T: DBRecord,
{
    /// Get the Thing (`surrealdb::sql::thing::Thing`) from the UUID.
    pub fn thing(&self) -> Thing {
        self.0.to_owned()
    }

    /// Get the UUID as a string.
    pub fn uuid_string(&self) -> String {
        match &self.0.id {
            Id::Uuid(uuid) => uuid.0.to_string(),
            Id::String(s) => s.to_owned(),
            _ => panic!("Invalid UUID type"),
        }
    }

    /// Create a new UUID with a random ID for the given table.
    pub fn new() -> Self {
        Thing::from((T::table().to_owned(), Id::from(Uuid::new_v4()))).into()
    }

    /// Get the object associated with the UUID.
    ///
    /// Returns an `Error` if SurrealDB unexpectedly fails.
    ///
    /// If a missing object should not result in an error, use `object_opt()` instead.
    #[allow(dead_code)]
    pub async fn object(&self, client: &Surreal<Client>) -> Result<T, SurrealSocketError>
    where
        T: DBRecord,
    {
        let opt = self.object_opt(client).await?;
        let obj = opt.ok_or_else(|| SurrealSocketError::new("Associated object not found"))?;
        Ok(obj)
    }

    /// Get the object associated with the UUID, or `None` if not found.
    pub async fn object_opt(
        &self,
        client: &Surreal<Client>,
    ) -> Result<Option<T>, SurrealSocketError>
    where
        T: DBRecord,
    {
        let obj: Option<T> = T::db_by_id(client, &self.uuid_string()).await?;
        Ok(obj)
    }
}

impl<T: DBRecord> Default for SsUuid<T> {
    fn default() -> Self {
        Thing::from((String::new(), Id::from(String::new()))).into()
    }
}

impl<T: DBRecord> Serialize for SsUuid<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}:{}", self.0.tb, self.uuid_string());
        serializer.serialize_str(&s)
    }
}

impl<'de, T: DBRecord> Deserialize<'de> for SsUuid<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UUIDVisitor<T>(PhantomData<T>);

        impl<T> serde::de::Visitor<'_> for UUIDVisitor<T> {
            type Value = SsUuid<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string in the format `table:uuid`")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let parts: Vec<&str> = value.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(E::custom("expected a string in the format `table:uuid`"));
                }
                Ok(SsUuid(Thing::from((parts[0], parts[1])), PhantomData))
            }
        }

        deserializer.deserialize_string(UUIDVisitor(PhantomData))
    }
}

impl<T: DBRecord> PartialEq for SsUuid<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[async_trait]
pub trait Expirable: DBRecord {
    fn start_time_field() -> &'static str;

    fn expiry_seconds() -> u64;

    fn start_timestamp(&self) -> Result<i64, SurrealSocketError> {
        let value = serde_json::to_value(self)?;
        let start_time_str = value
            .get(Self::start_time_field())
            .ok_or(SurrealSocketError::new(
                "start_time_field() does not match a property in an Expirable",
            ))?
            .as_str()
            .ok_or(SurrealSocketError::new(
                "start_time_field() does not match a string in an Expirable",
            ))?;

        let start_time = DateTime::parse_from_rfc3339(start_time_str).map_err(|e| {
            SurrealSocketError::new(&format!(
                "Error parsing start_time_field() as RFC3339: {}",
                e
            ))
        })?;

        Ok(start_time.timestamp())
    }

    async fn clear_expired(client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        let earliest_valid_time = Utc::now()
            .checked_sub_signed(Duration::seconds(Self::expiry_seconds() as i64))
            .ok_or(SurrealSocketError::new(
                "Out of bounds datetime in clear_expired()",
            ))?;

        Self::db_query(
            client,
            SQLCommand::Delete,
            format!("time::unix(type::datetime({}))", Self::start_time_field()),
            '<',
            earliest_valid_time.timestamp(),
        )
        .await?;

        Ok(())
    }

    fn is_expired(&self) -> Result<bool, SurrealSocketError> {
        let now = Utc::now().timestamp();
        let start_time = self.start_timestamp()?;
        Ok(now - start_time > Self::expiry_seconds() as i64)
    }
}
