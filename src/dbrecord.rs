use crate::error::SurrealSocketError;
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Serialize};
use serde::{Deserialize, Deserializer, Serializer};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use std::{
    any::Any,
    collections::HashMap,
    fmt::{Display, Formatter},
};
use surrealdb::engine::remote::ws::Client;
use surrealdb::sql::Thing;
use surrealdb::sql::{Id, Uuid};
use surrealdb::Surreal;

const CREATED_AT_FIELD: &str = "z_created_at";
const UPDATED_AT_FIELD: &str = "z_updated_at";
const DELETED_AT_FIELD: &str = "z_deleted_at";

#[derive(Serialize)]
pub struct SsTable(&'static str);

impl SsTable {
    pub fn new(table: &'static str) -> Self {
        SsTable(table)
    }
}

impl Display for SsTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Methods associated with SurrealDB tables
#[async_trait]
pub trait DBRecord: Any + Serialize + DeserializeOwned + Send + Sync {
    /// The associated table name
    const TABLE_NAME: &'static str;

    /// The name of the field that contains the UUID, used by `db_get_by_id()`
    const UUID_FIELD: &'static str = "uuid";

    /// Get the UUID associated with the record
    fn uuid(&self) -> SsUuid<Self>;

    /// Whether records should be moved to a table named `z_trashed_{table}` on `db_delete()`
    fn use_trash() -> bool {
        false
    }

    /// Defines records in other tables that should be cascade-deleted when this record is deleted.
    ///
    /// This is used to clean up related records that reference this record by a foreign key field.
    ///
    /// ### Example
    /// ```rust
    /// fn cascade_delete() -> Vec<CascadeDelete> {
    /// 	vec![cascade!(OtherModel, "name_of_field_that_references_this_model_by_ssuuid")]
    /// }
    /// ```
    fn cascade_delete() -> Vec<CascadeDelete> {
        vec![]
    }

    /// If the method returns an `Error`, the deletion will be aborted and `db_delete()` will return the error.
    async fn pre_delete_hook(&self) -> Result<(), SurrealSocketError> {
        Ok(())
    }

    /// If the method returns an `Error`, the `db_delete()` will return the error (but the deletion will still occur).
    async fn post_delete_hook(&self) -> Result<(), SurrealSocketError> {
        Ok(())
    }

    /// If the method returns an `Error`, the update will be aborted and `db_update()`/`db_create()` will return the error.
    async fn pre_update_hook(&self) -> Result<(), SurrealSocketError> {
        Ok(())
    }

    /// If the method returns an `Error`, the `db_update()`/`db_create()` will return the error (but the update will still occur).
    async fn post_update_hook(&self) -> Result<(), SurrealSocketError> {
        Ok(())
    }

    /// If the method returns an `Error`, the creation will be aborted and `db_create()` will return the error.
    async fn pre_create_hook(&self) -> Result<(), SurrealSocketError> {
        Ok(())
    }

    /// If the method returns an `Error`, the `db_create()` will return the error (but the creation will still occur).
    async fn post_create_hook(&self) -> Result<(), SurrealSocketError> {
        Ok(())
    }

    fn table() -> SsTable {
        SsTable::new(Self::TABLE_NAME)
    }

    /// Get the Thing (`surrealdb::sql::thing::Thing`) associated with the record
    fn thing(&self) -> Thing {
        self.uuid().as_thing().to_owned()
    }

    /// Get an object from SurrealDB by its ID, or `None` if not found.
    ///
    /// Returns an `Error` if SurrealDB unexpectedly fails.
    async fn db_get_by_id(
        client: &Surreal<Client>,
        id: &str,
    ) -> Result<Option<Self>, SurrealSocketError> {
        let thing = Thing::from((Self::table().to_string(), id.to_owned()));
        let uuid: SsUuid<Self> = SsUuid::from(thing);
        let item: Option<Self> = Self::db_search_one(client, Self::UUID_FIELD, uuid).await?;
        Ok(item)
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
        let query =
            format!("{sql_command} FROM type::table($table) WHERE {field} {operand} $value");

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
        self.pre_create_hook().await?;
        self.pre_update_hook().await?;
        let serde_value = serde_json::to_value(self)?;

        let mut serde_value = match serde_value.as_object() {
            Some(obj) => obj.clone(),
            None => {
                return Err(SurrealSocketError::new(
                    "Failed to convert the object to a Map",
                ))
            }
        };

        serde_value.insert(
            CREATED_AT_FIELD.to_owned(),
            serde_json::to_value(Utc::now())?,
        );

        serde_value.insert(
            UPDATED_AT_FIELD.to_owned(),
            serde_json::to_value(Utc::now())?,
        );

        let id = self.uuid().to_uuid_string();

        let opt: Option<Self> = client
            .create((Self::table().to_string(), id.to_owned()))
            .content(serde_value)
            .await?;

        self.post_create_hook().await?;
        self.post_update_hook().await?;

        match opt {
            Some(e) => Ok(e),
            None => Err(SurrealSocketError::new("Failed to create record")),
        }
    }

    /// Delete a record from the database.
    async fn db_delete(&self, client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        self.pre_delete_hook().await?;

        for ref_ in Self::cascade_delete() {
            (ref_.func)(Arc::new(client.clone()), self.uuid().to_string()).await?;
        }

        if Self::use_trash() {
            let serde_value = serde_json::to_value(self)?;

            let mut serde_value = match serde_value.as_object() {
                Some(obj) => obj.clone(),
                None => {
                    return Err(SurrealSocketError::new(
                        "Failed to convert the object to a Map",
                    ))
                }
            };

            serde_value.insert(
                DELETED_AT_FIELD.to_owned(),
                serde_json::to_value(Utc::now())?,
            );

            let created: Option<Self> = client
                .create((
                    format!("z_trashed_{}", Self::table()),
                    self.uuid().to_uuid_string(),
                ))
                .content(serde_value)
                .await?;

            created.ok_or_else(|| {
                SurrealSocketError::new(&format!(
                    "Failed to create trash table record: {}",
                    self.uuid().to_uuid_string()
                ))
            })?;
        }

        let thing = self.thing();

        let _: Option<Self> = client
            .delete((thing.tb, self.uuid().to_uuid_string()))
            .await?;

        self.post_delete_hook().await?;
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
    ///
    /// Use serde_json::json!() for the values if they are different types.
    async fn db_update_fields<T: Serialize + Sync + Send + Clone>(
        &self,
        client: &Surreal<Client>,
        updates: Vec<(&str, T)>,
    ) -> Result<(), SurrealSocketError> {
        self.pre_update_hook().await?;
        let mut merge_data = HashMap::<String, serde_json::Value>::new();

        merge_data.insert(
            UPDATED_AT_FIELD.to_owned(),
            serde_json::to_value(Utc::now())?,
        );

        for (k, v) in updates {
            merge_data.insert(k.to_owned(), serde_json::to_value(v)?);
        }

        let _: Option<Self> = client
            .update((Self::table().to_string(), self.uuid().to_uuid_string()))
            .merge(merge_data)
            .await?;

        self.post_update_hook().await?;
        Ok(())
    }

    async fn db_overwrite(&self, client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        self.pre_update_hook().await?;
        let serde_value = serde_json::to_value(self)?;
        let id = self.uuid().to_uuid_string();

        let mut serde_value = match serde_value.as_object() {
            Some(obj) => obj.clone(),
            None => return Err(SurrealSocketError::new("Failed to convert object to Map")),
        };

        serde_value.insert(
            UPDATED_AT_FIELD.to_owned(),
            serde_json::to_value(Utc::now())?,
        );

        let _: Option<Self> = client
            .update((Self::table().to_string(), id))
            .merge(serde_value)
            .await?;

        self.post_update_hook().await?;
        Ok(())
    }

    async fn db_all(client: &Surreal<Client>) -> Result<Vec<Self>, SurrealSocketError> {
        client.set("table", Self::table()).await?;
        let mut response = client.query("SELECT * FROM type::table($table)").await?;
        let result: Vec<Self> = response.take(0)?;
        Ok(result)
    }

    /// For each record in the table, add any missing properties with default values to the record in the database.
    ///
    /// Record retrieval already uses default values for missing fields, but this exists just in case it's ever needed.
    async fn db_populate_defaults(client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        let result = Self::db_all(client).await?;
        let table = Self::table();

        for item in result {
            let _: Option<Self> = client
                .update((table.to_string(), item.uuid().to_uuid_string()))
                .merge(item)
                .await?;
        }

        Ok(())
    }

    async fn db_drop_table(client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        let table_string = Self::table().to_string();
        client.query(format!("REMOVE TABLE {table_string}")).await?;
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

impl<T: DBRecord> FromStr for SsUuid<T> {
    type Err = SurrealSocketError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::from_str(s)
            .map_err(|_| SurrealSocketError::new(&format!("Invalid UUID string: {s}")))?;

        Ok(Thing::from((T::table().to_string(), Id::from(uuid))).into())
    }
}

impl<T> SsUuid<T>
where
    T: DBRecord,
{
    /// Get the Thing (`surrealdb::sql::thing::Thing`) from the SsUuid.
    pub fn as_thing(&self) -> &Thing {
        &self.0
    }

    /// Get the SsUuid's uuid component as a string (eg: "87e4f33a-e9e1-411c-8f74-9f6f1098096e")
    ///
    /// Use to_string() to include the table name (eg: "users:87e4f33a-e9e1-411c-8f74-9f6f1098096e")
    pub fn to_uuid_string(&self) -> String {
        match &self.0.id {
            Id::Uuid(uuid) => uuid.0.to_string(),
            Id::String(s) => s.to_owned(),
            _ => panic!("Invalid UUID type"),
        }
    }

    /// Create a new SsUuid with a random ID for the given table.
    pub fn new() -> Self {
        Thing::from((T::table().to_string(), Id::from(Uuid::new_v4()))).into()
    }

    /// Get the object associated with the SsUuid.
    ///
    /// Returns an `Error` if SurrealDB unexpectedly fails.
    ///
    /// If a missing object should not result in an error, use `db_fetch_opt()` instead.
    pub async fn db_fetch(&self, client: &Surreal<Client>) -> Result<T, SurrealSocketError>
    where
        T: DBRecord,
    {
        let opt = self.db_fetch_opt(client).await?;
        let obj = opt.ok_or_else(|| {
            SurrealSocketError::new(&format!(
                "Associated object of type `{}` not found",
                std::any::type_name::<T>()
            ))
        })?;
        Ok(obj)
    }

    /// Get the object associated with the SsUuid, or `None` if not found.
    pub async fn db_fetch_opt(
        &self,
        client: &Surreal<Client>,
    ) -> Result<Option<T>, SurrealSocketError>
    where
        T: DBRecord,
    {
        let obj: Option<T> = T::db_get_by_id(client, &self.to_uuid_string()).await?;
        Ok(obj)
    }
}

impl<T: DBRecord> Display for SsUuid<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0.tb, self.to_uuid_string())
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
        let s = format!("{}:{}", self.0.tb, self.to_uuid_string());
        serializer.serialize_str(&s)
    }
}

impl<'de, T: DBRecord> Deserialize<'de> for SsUuid<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UUIDVisitor<T>(PhantomData<T>);

        impl<'de, T> serde::de::Visitor<'de> for UUIDVisitor<T> {
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
    // As either DateTime or NaiveDate
    fn start_time_field() -> &'static str;

    fn expiry_seconds() -> u64;

    fn start_datetime(&self) -> Result<DateTime<Utc>, SurrealSocketError> {
        let value = serde_json::to_value(self)?;
        let start_time_str = value
            .get(Self::start_time_field())
            .ok_or_else(|| SurrealSocketError::new("Missing start_time_field in record"))?
            .as_str()
            .ok_or_else(|| SurrealSocketError::new("start_time_field must be a string"))?;

        // RFC3339 / DateTime
        if let Ok(dt) = DateTime::parse_from_rfc3339(start_time_str) {
            return Ok(dt.with_timezone(&Utc));
        }

        // NaiveDate
        if let Ok(d) = NaiveDate::parse_from_str(start_time_str, "%Y-%m-%d") {
            return Ok(DateTime::from_naive_utc_and_offset(
                d.and_hms_opt(0, 0, 0).unwrap(),
                Utc,
            ));
        }

        Err(SurrealSocketError::new("Invalid date format"))
    }

    fn start_timestamp(&self) -> Result<i64, SurrealSocketError> {
        Ok(self.start_datetime()?.timestamp())
    }

    fn is_expired(&self) -> Result<bool, SurrealSocketError> {
        let now = Utc::now().timestamp();
        Ok(now - self.start_timestamp()? > Self::expiry_seconds() as i64)
    }

    async fn clear_expired(client: &Surreal<Client>) -> Result<(), SurrealSocketError> {
        let earliest_valid_time = Utc::now()
            .checked_sub_signed(Duration::seconds(Self::expiry_seconds() as i64))
            .ok_or_else(|| SurrealSocketError::new("Datetime underflow"))?;

        let query = format!("time::unix(type::datetime({}))", Self::start_time_field());

        Self::db_query(
            client,
            SQLCommand::Delete,
            query,
            '<',
            earliest_valid_time.timestamp(),
        )
        .await?;

        Ok(())
    }
}

pub fn cascade_by<T: DBRecord + 'static>(field: &'static str) -> CascadeFn {
    Arc::new(move |client: Arc<Surreal<Client>>, uuid: String| {
        Box::pin(async move {
            let records = T::db_search(&client, field, uuid.clone()).await?;

            for rec in records {
                rec.db_delete(&client).await?;
            }

            Ok(())
        })
    })
}

pub struct CascadeDelete {
    /// The name of the field in the referencing table that references this record
    pub field: &'static str,
    /// The function that performs the deletion (`cascade_by()`)
    pub func: CascadeFn,
}

pub type CascadeFn = Arc<
    dyn Fn(Arc<Surreal<Client>>, String) -> BoxFuture<'static, Result<(), SurrealSocketError>>
        + Send
        + Sync,
>;

#[macro_export]
macro_rules! cascade {
    ($type:ty, $field:literal) => {
        $crate::dbrecord::CascadeDelete {
            field: $field,
            func: $crate::dbrecord::cascade_by::<$type>($field),
        }
    };
}
