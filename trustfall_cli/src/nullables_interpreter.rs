use std::sync::Arc;

use serde::{Deserialize, Serialize};

use trustfall_core::{
    interpreter::{Adapter, DataContext, InterpretedQuery},
    ir::{EdgeParameters, Eid, FieldValue, Vid},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NullablesToken;

#[derive(Debug, Clone)]
pub struct NullablesAdapter;

#[allow(unused_variables)]
impl Adapter<'static> for NullablesAdapter {
    type DataToken = NullablesToken;

    fn get_starting_tokens(
        &self,
        edge: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = Self::DataToken>> {
        unimplemented!()
    }

    fn project_property(
        &self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>>>,
        current_type_name: Arc<str>,
        field_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = (DataContext<Self::DataToken>, FieldValue)>> {
        unimplemented!()
    }

    #[allow(clippy::type_complexity)]
    fn project_neighbors(
        &self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>>>,
        current_type_name: Arc<str>,
        edge_name: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
        edge_hint: Eid,
    ) -> Box<
        dyn Iterator<
            Item = (
                DataContext<Self::DataToken>,
                Box<dyn Iterator<Item = Self::DataToken>>,
            ),
        >,
    > {
        unimplemented!()
    }

    fn can_coerce_to_type(
        &self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>>>,
        current_type_name: Arc<str>,
        coerce_to_type_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = (DataContext<Self::DataToken>, bool)>> {
        unimplemented!()
    }
}
