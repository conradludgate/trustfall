use std::{cmp::Ordering, collections::BTreeMap, fmt::Debug, sync::Arc};

use async_graphql_parser::types::Type;
use dbg_pls::DebugPls;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    ir::{types::is_argument_type_valid, EdgeParameters, Eid, FieldValue, FoldSpecificField, Vid},
    util::BTreeMapTryInsertExt,
};

use self::error::QueryArgumentsError;

pub mod basic_adapter;
pub mod error;
pub mod execution;
mod filtering;
pub mod macros;
pub mod query_plan;
pub mod replay;
pub mod trace;

#[doc(hidden)]
pub mod executer_components;

#[derive(Debug, Clone)]
pub struct DataContext<DataToken: Clone + Debug> {
    pub current_token: Option<DataToken>,
    tokens: BTreeMap<Vid, Option<DataToken>>,
    values: Vec<FieldValue>,
    suspended_tokens: Vec<Option<DataToken>>,
    folded_contexts: BTreeMap<Eid, Vec<DataContext<DataToken>>>,
    folded_values: BTreeMap<(Eid, Arc<str>), Option<ValueOrVec>>,
    piggyback: Option<Vec<DataContext<DataToken>>>,
    imported_tags: BTreeMap<FieldRef, FieldValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DebugPls)]
pub enum FieldRef {
    ContextField(ContextField),
    FoldSpecificField(FoldSpecificField),
}

impl Ord for FieldRef {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (FieldRef::ContextField(f1), FieldRef::ContextField(f2)) => f1
                .vertex_id
                .cmp(&f2.vertex_id)
                .then(f1.field_name.as_ref().cmp(f2.field_name.as_ref())),
            (FieldRef::ContextField(_), FieldRef::FoldSpecificField(_)) => Ordering::Less,
            (FieldRef::FoldSpecificField(_), FieldRef::ContextField(_)) => Ordering::Greater,
            (FieldRef::FoldSpecificField(f1), FieldRef::FoldSpecificField(f2)) => {
                f1.fold_eid.cmp(&f2.fold_eid).then(f1.kind.cmp(&f2.kind))
            }
        }
    }
}
impl PartialOrd for FieldRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<crate::ir::ContextField> for ContextField {
    fn from(c: crate::ir::ContextField) -> Self {
        ContextField {
            vertex_id: c.vertex_id,
            field_name: c.field_name,
        }
    }
}
impl From<crate::ir::FieldRef> for FieldRef {
    fn from(value: crate::ir::FieldRef) -> Self {
        match value {
            crate::ir::FieldRef::ContextField(c) => FieldRef::ContextField(c.into()),
            crate::ir::FieldRef::FoldSpecificField(f) => FieldRef::FoldSpecificField(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DebugPls)]
pub struct ContextField {
    pub vertex_id: Vid,
    pub field_name: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[doc(hidden)]
pub enum ValueOrVec {
    Value(FieldValue),
    Vec(Vec<ValueOrVec>),
}

impl ValueOrVec {
    fn as_mut_vec(&mut self) -> Option<&mut Vec<ValueOrVec>> {
        match self {
            ValueOrVec::Value(_) => None,
            ValueOrVec::Vec(v) => Some(v),
        }
    }
}

impl From<ValueOrVec> for FieldValue {
    fn from(v: ValueOrVec) -> Self {
        match v {
            ValueOrVec::Value(value) => value,
            ValueOrVec::Vec(v) => v.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "DataToken: Serialize, for<'de2> DataToken: Deserialize<'de2>")]
struct SerializableContext<DataToken>
where
    DataToken: Clone + Debug + Serialize,
    for<'d> DataToken: Deserialize<'d>,
{
    current_token: Option<DataToken>,
    tokens: BTreeMap<Vid, Option<DataToken>>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    values: Vec<FieldValue>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    suspended_tokens: Vec<Option<DataToken>>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    folded_contexts: BTreeMap<Eid, Vec<DataContext<DataToken>>>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    folded_values: BTreeMap<(Eid, Arc<str>), Option<ValueOrVec>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    piggyback: Option<Vec<DataContext<DataToken>>>,

    /// Tagged values imported from an ancestor component of the one currently being evaluated.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    imported_tags: BTreeMap<FieldRef, FieldValue>,
}

impl<DataToken> From<SerializableContext<DataToken>> for DataContext<DataToken>
where
    DataToken: Clone + Debug + Serialize,
    for<'d> DataToken: Deserialize<'d>,
{
    fn from(context: SerializableContext<DataToken>) -> Self {
        Self {
            current_token: context.current_token,
            tokens: context.tokens,
            values: context.values,
            suspended_tokens: context.suspended_tokens,
            folded_contexts: context.folded_contexts,
            folded_values: context.folded_values,
            piggyback: context.piggyback,
            imported_tags: context.imported_tags,
        }
    }
}

impl<DataToken> From<DataContext<DataToken>> for SerializableContext<DataToken>
where
    DataToken: Clone + Debug + Serialize,
    for<'d> DataToken: Deserialize<'d>,
{
    fn from(context: DataContext<DataToken>) -> Self {
        Self {
            current_token: context.current_token,
            tokens: context.tokens,
            values: context.values,
            suspended_tokens: context.suspended_tokens,
            folded_contexts: context.folded_contexts,
            folded_values: context.folded_values,
            piggyback: context.piggyback,
            imported_tags: context.imported_tags,
        }
    }
}

impl<DataToken: Clone + Debug> DataContext<DataToken> {
    pub fn new(token: DataToken) -> DataContext<DataToken> {
        DataContext {
            current_token: Some(token),
            piggyback: None,
            tokens: Default::default(),
            values: Default::default(),
            suspended_tokens: Default::default(),
            folded_contexts: Default::default(),
            folded_values: Default::default(),
            imported_tags: Default::default(),
        }
    }

    fn record_token(&mut self, vid: Vid) {
        self.tokens
            .insert_or_error(vid, self.current_token.clone())
            .unwrap();
    }

    fn activate_token(mut self, vid: &Vid) -> DataContext<DataToken> {
        self.current_token = self.tokens[vid].clone();
        self
    }

    fn split_and_move_to_token(&self, new_token: Option<DataToken>) -> DataContext<DataToken> {
        DataContext {
            current_token: new_token,
            tokens: self.tokens.clone(),
            values: self.values.clone(),
            suspended_tokens: self.suspended_tokens.clone(),
            folded_contexts: self.folded_contexts.clone(),
            folded_values: self.folded_values.clone(),
            piggyback: None,
            imported_tags: self.imported_tags.clone(),
        }
    }

    fn unsuspend(mut self) -> DataContext<DataToken> {
        self.current_token = self.suspended_tokens.pop().unwrap();
        self
    }

    fn ensure_suspended(mut self) -> DataContext<DataToken> {
        if let Some(token) = self.current_token.take() {
            self.suspended_tokens.push(Some(token));
        }
        self
    }

    fn ensure_unsuspended(mut self) -> DataContext<DataToken> {
        self.current_token = self
            .current_token
            .or_else(|| self.suspended_tokens.pop().unwrap());
        self
    }
}

impl<DataToken: Debug + Clone + PartialEq> PartialEq for DataContext<DataToken> {
    fn eq(&self, other: &Self) -> bool {
        self.current_token == other.current_token
            && self.tokens == other.tokens
            && self.values == other.values
            && self.suspended_tokens == other.suspended_tokens
            && self.folded_contexts == other.folded_contexts
            && self.piggyback == other.piggyback
            && self.imported_tags == other.imported_tags
    }
}

impl<DataToken: Debug + Clone + PartialEq + Eq> Eq for DataContext<DataToken> {}

impl<DataToken> Serialize for DataContext<DataToken>
where
    DataToken: Debug + Clone + Serialize,
    for<'d> DataToken: Deserialize<'d>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // TODO: eventually maybe write a proper (de)serialize?
        SerializableContext::from(self.clone()).serialize(serializer)
    }
}

impl<'de, DataToken> Deserialize<'de> for DataContext<DataToken>
where
    DataToken: Debug + Clone + Serialize,
    for<'d> DataToken: Deserialize<'d>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // TODO: eventually maybe write a proper (de)serialize?
        SerializableContext::deserialize(deserializer).map(DataContext::from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterpretedQuery {
    pub arguments: Arc<BTreeMap<Arc<str>, FieldValue>>,
}

impl InterpretedQuery {
    #[inline]
    pub fn from_query_and_arguments(
        variables: &BTreeMap<Arc<str>, Type>,
        arguments: Arc<BTreeMap<Arc<str>, FieldValue>>,
    ) -> Result<Self, QueryArgumentsError> {
        let mut errors = vec![];

        let mut missing_arguments = vec![];
        for (variable_name, variable_type) in variables {
            match arguments.get(variable_name) {
                Some(argument_value) => {
                    // Ensure the provided argument value is valid for the variable's inferred type.
                    if let Err(e) = validate_argument_type(
                        variable_name.as_ref(),
                        variable_type,
                        argument_value,
                    ) {
                        errors.push(e);
                    }
                }
                None => {
                    missing_arguments.push(variable_name.as_ref());
                }
            }
        }
        if !missing_arguments.is_empty() {
            errors.push(QueryArgumentsError::MissingArguments(
                missing_arguments
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect(),
            ));
        }

        let unused_arguments = arguments
            .keys()
            .map(|x| x.as_ref())
            .filter(|arg| !variables.contains_key(*arg))
            .collect_vec();
        if !unused_arguments.is_empty() {
            errors.push(QueryArgumentsError::UnusedArguments(
                unused_arguments
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect(),
            ));
        }

        if errors.is_empty() {
            Ok(Self { arguments })
        } else {
            Err(errors.into())
        }
    }
}

fn validate_argument_type(
    variable_name: &str,
    variable_type: &Type,
    argument_value: &FieldValue,
) -> Result<(), QueryArgumentsError> {
    if is_argument_type_valid(variable_type, argument_value) {
        Ok(())
    } else {
        Err(QueryArgumentsError::ArgumentTypeError(
            variable_name.to_string(),
            variable_type.to_string(),
            argument_value.to_owned(),
        ))
    }
}

type Iter<'token, Item> = Box<dyn Iterator<Item = Item> + 'token>;

pub trait Adapter<'token> {
    type DataToken: Clone + Debug + 'token;

    fn get_starting_tokens(
        &self,
        edge: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = Self::DataToken> + 'token>;

    /// Properties are one to one mappings between a type and it's field contents.
    fn project_property(
        &self,
        data_contexts: Iter<'token, DataContext<Self::DataToken>>,
        current_type_name: Arc<str>,
        field_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Iter<'token, (DataContext<Self::DataToken>, FieldValue)>;

    /// Neighbors are one to many mappings between a type and another type, following the edge
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    fn project_neighbors(
        &self,
        data_contexts: Iter<'token, DataContext<Self::DataToken>>,
        current_type_name: Arc<str>,
        edge_name: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
        edge_hint: Eid,
    ) -> Iter<'token, (DataContext<Self::DataToken>, Iter<'token, Self::DataToken>)>;

    /// Maps whether the current value can coerce into the desired type
    fn can_coerce_to_type(
        &self,
        data_contexts: Iter<'token, DataContext<Self::DataToken>>,
        current_type_name: Arc<str>,
        coerce_to_type_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Iter<'token, (DataContext<Self::DataToken>, bool)>;
}
