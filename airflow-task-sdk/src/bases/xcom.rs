extern crate alloc;
use airflow_common::serialization::serde::JsonValue;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::string::ToString;
use core::{error::Error, marker::PhantomData};

use airflow_common::serialization::serde::{
    JsonDeserialize, JsonSerdeError, JsonSerialize, deserialize, serialize,
};
use airflow_common::utils::MapIndex;

use crate::execution::TaskRuntime;
use crate::{
    api::datamodels::XComResponse,
    execution::{SupervisorClient, SupervisorCommsError},
};

pub static XCOM_RETURN_KEY: &str = "return_value";

pub trait XComValue: JsonSerialize + JsonDeserialize + Send + Sync {}

impl<T> XComValue for T where T: JsonSerialize + JsonDeserialize + Send + Sync {}

impl<T> From<T> for Box<dyn XComValue>
where
    T: XComValue + 'static,
{
    fn from(value: T) -> Self {
        Box::new(value)
    }
}

impl JsonSerialize for Box<dyn XComValue> {
    fn serialize(&self) -> Result<String, JsonSerdeError> {
        self.as_ref().serialize()
    }
}

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
    async fn deserialize_value<T: XComValue>(xcom: &XComResponse) -> Result<T, Self::Error>;

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

    async fn deserialize_value<T: XComValue>(xcom: &XComResponse) -> Result<T, Self::Error> {
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
}

#[derive(Default)]
pub struct XCom<X: XComBackend>(PhantomData<X>);

impl<X: XComBackend> XCom<X> {
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
