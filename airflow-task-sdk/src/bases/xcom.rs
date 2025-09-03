extern crate alloc;
use crate::{
    api::datamodels::XComResponse,
    execution::{RuntimeTaskInstance, SupervisorClient, SupervisorCommsError, TaskRuntime},
};
use airflow_common::{
    models::TaskInstanceLike,
    serialization::serde::{
        JsonDeserialize, JsonSerdeError, JsonSerialize, JsonValue, deserialize, serialize,
    },
    utils::MapIndex,
};
use alloc::{string::ToString, vec::Vec};
use core::{error::Error, marker::PhantomData};
use tracing::info;

pub static XCOM_RETURN_KEY: &str = "return_value";

#[trait_variant::make(Send)]
pub trait XComBackend {
    type Error: Error;

    /// Serialize XCom value to JSON.
    async fn serialize_value<T: JsonSerialize + Sync>(
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        map_index: MapIndex,
        key: &str,
        value: &T,
    ) -> Result<JsonValue, Self::Error>;

    /// Deserialize XCom value from JSON.
    async fn deserialize_value<T: JsonDeserialize>(xcom: &XComResponse) -> Result<T, Self::Error>;

    /// Purge an XCom entry from underlying storage implementations.
    async fn purge(xcom: &XComResponse) -> Result<(), Self::Error>;
}

#[derive(Debug, Default)]
pub struct BaseXcom;

impl XComBackend for BaseXcom {
    type Error = JsonSerdeError;

    async fn serialize_value<T: JsonSerialize>(
        _dag_id: &str,
        _run_id: &str,
        _task_id: &str,
        _map_index: MapIndex,
        _key: &str,
        value: &T,
    ) -> Result<JsonValue, Self::Error> {
        serialize(value)
    }

    async fn deserialize_value<T: JsonDeserialize>(xcom: &XComResponse) -> Result<T, Self::Error> {
        deserialize(&xcom.value)
    }

    async fn purge(_xcom: &XComResponse) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum XComError<X: XComBackend> {
    #[error(transparent)]
    Serde(#[from] JsonSerdeError),
    #[error(transparent)]
    Backend(X::Error),
    #[error(transparent)]
    Comms(#[from] SupervisorCommsError),
    #[error("Did not push XCom for task mapping")]
    XComForMappingNotPushed,
    #[error("Unmappable XCom type pushed: {0}")]
    UnmappableXComTypePushed(JsonValue),
    #[error("Non-object XCom type pushed with multiple_outputs=true: {0}")]
    NonObjectXComTypePushed(JsonValue),
}

#[derive(Default)]
pub struct XCom<X: XComBackend>(PhantomData<X>);

impl<X: XComBackend> XCom<X> {
    /// Store an XCom value.
    #[allow(clippy::too_many_arguments)]
    pub async fn set<T: JsonSerialize + Sync, R: TaskRuntime>(
        client: &SupervisorClient<R>,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        map_index: MapIndex,
        key: &str,
        value: &T,
        mapped_length: Option<usize>,
    ) -> Result<(), XComError<X>> {
        let value = X::serialize_value(dag_id, run_id, task_id, map_index, key, value)
            .await
            .map_err(XComError::Backend)?;

        client
            .set_xcom(
                key.to_string(),
                value,
                dag_id.to_string(),
                run_id.to_string(),
                task_id.to_string(),
                Some(map_index),
                mapped_length,
            )
            .await?;
        Ok(())
    }

    /// Store an XCom value directly in the metadata database.
    pub(crate) async fn _set_xcom_in_db<R: TaskRuntime>(
        client: &SupervisorClient<R>,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        map_index: MapIndex,
        key: &str,
        value: &JsonValue,
    ) -> Result<(), XComError<X>> {
        client
            .set_xcom(
                key.to_string(),
                value.clone(),
                dag_id.to_string(),
                run_id.to_string(),
                task_id.to_string(),
                Some(map_index),
                None,
            )
            .await?;
        Ok(())
    }

    /// Retrieve an XCom value for a task instance like.
    ///
    /// This is an alternative to [XCom::get_one] for convenience purposes
    /// if there's already a [TaskInstanceLike] available.
    pub async fn get_value<T: JsonDeserialize + Send, R: TaskRuntime>(
        client: &SupervisorClient<R>,
        ti: &impl TaskInstanceLike,
        key: &str,
    ) -> Result<T, XComError<X>> {
        XCom::<X>::get_one(
            client,
            ti.dag_id(),
            ti.run_id(),
            ti.task_id(),
            ti.map_index(),
            key,
            None,
        )
        .await
    }

    /// Retrieve an XCom value directly from the metadata database.
    pub(crate) async fn get_xcom_db_ref<R: TaskRuntime>(
        client: &SupervisorClient<R>,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        map_index: MapIndex,
        key: &str,
    ) -> Result<XComResponse, XComError<X>> {
        let response = client
            .get_xcom(
                key.to_string(),
                dag_id.to_string(),
                run_id.to_string(),
                task_id.to_string(),
                Some(map_index),
                None,
            )
            .await?;
        Ok(response)
    }

    /// Retrieve an XCom value.
    ///
    /// See also: [XCom::get_value] is a convenience function if you already
    /// have a [TaskInstanceLike] available.
    pub async fn get_one<T: JsonDeserialize + Send, R: TaskRuntime>(
        client: &SupervisorClient<R>,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        map_index: MapIndex,
        key: &str,
        include_prior_dates: Option<bool>,
    ) -> Result<T, XComError<X>> {
        let response = client
            .get_xcom(
                key.to_string(),
                dag_id.to_string(),
                run_id.to_string(),
                task_id.to_string(),
                Some(map_index),
                include_prior_dates,
            )
            .await?;

        if response.value.is_null() {
            // we just show the warning and continue as usual, because defaults
            // are implemented by deserializing Options.
            info!(
                key = key,
                dag_id = dag_id,
                task_id = task_id,
                run_id = run_id,
                map_index = map_index.to_string(),
                "No XCom value found; defaulting to None."
            );
        }

        let result = deserialize(&response.value).map_err(XComError::Serde)?;
        Ok(result)
    }

    /// Retrieve all XCom values for a task, typically from all map indexes.
    pub async fn get_all<T: JsonDeserialize + Send, R: TaskRuntime>(
        client: &SupervisorClient<R>,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        include_prior_dates: Option<bool>,
    ) -> Result<Vec<T>, XComError<X>> {
        let response = client
            .get_xcom_sequence_slice(
                key.to_string(),
                dag_id.to_string(),
                run_id.to_string(),
                task_id.to_string(),
                None,
                None,
                None,
                include_prior_dates,
            )
            .await?;
        let result = response
            .iter()
            .map(|v| deserialize(v).map_err(XComError::Serde))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result)
    }

    /// Delete an Xcom entry, for custom xcom backends, it gets the path associated with the data on the backend and purges it.
    pub async fn delete<R: TaskRuntime>(
        client: &SupervisorClient<R>,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        map_index: MapIndex,
        key: &str,
    ) -> Result<(), XComError<X>> {
        let result = XCom::get_xcom_db_ref(client, dag_id, run_id, task_id, map_index, key).await?;

        X::purge(&result).await.map_err(XComError::Backend)?;
        client
            .delete_xcom(
                key.to_string(),
                dag_id.to_string(),
                run_id.to_string(),
                task_id.to_string(),
                Some(map_index),
            )
            .await?;

        Ok(())
    }
}

/// Builder for pulling XCom values.
pub struct XComPull<'t, R: TaskRuntime, M = ()> {
    ti: &'t RuntimeTaskInstance<'t, R>,
    dag_id: &'t str,
    run_id: &'t str,
    task_id: &'t str,
    map_index: M,
    key: &'t str,
    include_prior_dates: Option<bool>,
}

// map_index not set
impl<'t, R: TaskRuntime> XComPull<'t, R> {
    pub(crate) fn new(ti: &'t RuntimeTaskInstance<'t, R>) -> Self {
        Self {
            ti,
            dag_id: ti.dag_id(),
            run_id: ti.run_id(),
            task_id: ti.task_id(),
            map_index: (),
            key: XCOM_RETURN_KEY,
            include_prior_dates: None,
        }
    }

    /// Set the map index for the XCom pull.
    pub fn map_index(self, map_index: MapIndex) -> XComPull<'t, R, MapIndex> {
        XComPull {
            ti: self.ti,
            dag_id: self.dag_id,
            run_id: self.run_id,
            task_id: self.task_id,
            map_index,
            key: self.key,
            include_prior_dates: self.include_prior_dates,
        }
    }

    /// Retrieve a single XCom value for a non-mapped task (i.e. `map_index = -1`).
    pub async fn one<T: JsonDeserialize + Send>(self) -> Result<T, XComError<BaseXcom>> {
        let result = XCom::<BaseXcom>::get_one(
            self.ti.client,
            self.dag_id,
            self.run_id,
            self.task_id,
            MapIndex::none(),
            self.key,
            self.include_prior_dates,
        )
        .await?;
        Ok(result)
    }

    /// Retrieve all XCom values for a task, i.e. from all map indexes for mapped tasks.
    pub async fn all<T: JsonDeserialize + Send>(self) -> Result<Vec<T>, XComError<BaseXcom>> {
        let result = XCom::<BaseXcom>::get_all(
            self.ti.client,
            self.dag_id,
            self.run_id,
            self.task_id,
            self.key,
            self.include_prior_dates,
        )
        .await?;
        Ok(result)
    }
}

// map_index set
impl<'t, R: TaskRuntime> XComPull<'t, R, MapIndex> {
    /// Retrieve a single XCom value for a mapped task (i.e. `map_index` is set to non-negative value).
    pub async fn one<T: JsonDeserialize + Send>(self) -> Result<T, XComError<BaseXcom>> {
        let result = XCom::<BaseXcom>::get_one(
            self.ti.client,
            self.dag_id,
            self.run_id,
            self.task_id,
            self.map_index,
            self.key,
            self.include_prior_dates,
        )
        .await?;
        Ok(result)
    }
}

// any map_index setting
impl<'t, R: TaskRuntime, M> XComPull<'t, R, M> {
    /// Set the DAG ID for the XCom pull.
    pub fn dag_id(mut self, dag_id: &'t str) -> Self {
        self.dag_id = dag_id;
        self
    }

    /// Set the run ID for the XCom pull.
    pub fn run_id(mut self, run_id: &'t str) -> Self {
        self.run_id = run_id;
        self
    }

    /// Set the task ID for the XCom pull.
    pub fn task_id(mut self, task_id: &'t str) -> Self {
        self.task_id = task_id;
        self
    }

    /// Set the key for the XCom pull.
    pub fn key(mut self, key: &'t str) -> Self {
        self.key = key;
        self
    }

    /// Set the include prior dates flag for the XCom pull.
    pub fn include_prior_dates(mut self, include_prior_dates: bool) -> Self {
        self.include_prior_dates = Some(include_prior_dates);
        self
    }
}
