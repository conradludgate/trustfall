#![allow(unused_variables, dead_code, unreachable_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::{fmt::Debug, marker::PhantomData};

use crate::ir::{Argument, IRVertex, Operation, ContextField};
use crate::{
    interpreter::basic_adapter::{ContextIterator, ContextOutcomeIterator},
    ir::{Eid, FieldValue, IRQueryComponent, Vid},
};

use super::{InterpretedQuery, Adapter};

mod candidates;
pub use candidates::{CandidateValue, RangeBoundKind, RangeEndpoint};

pub trait VertexInfo {
    fn current_component(&self) -> &IRQueryComponent;

    fn current_vertex(&self) -> &IRVertex;

    fn query_arguments(&self) -> &BTreeMap<Arc<str>, FieldValue>;

    fn coerced_to_type(&self) -> Option<&Arc<str>> {
        let vertex = self.current_vertex();
        if vertex.coerced_from_type.is_some() {
            Some(&vertex.type_name)
        } else {
            None
        }
    }

    fn static_field_value(&self, field_name: &str) -> Option<CandidateValue<'_>> {
        let vertex = self.current_vertex();

        let is_null = vertex
            .filters
            .iter()
            .any(|op| matches!(op, Operation::IsNull(..)));
        let is_not_null = vertex
            .filters
            .iter()
            .any(|op| matches!(op, Operation::IsNotNull(..)));

        if is_null && is_not_null {
            // The value can't be both null and non-null at the same time.
            return Some(CandidateValue::Impossible);
        }

        let mut candidate = if is_null {
            Some(CandidateValue::Single(&FieldValue::NULL))
        } else {
            None
        };

        let arguments = self.query_arguments();
        for filter_operation in &vertex.filters {
            match filter_operation {
                Operation::Equals(_, Argument::Variable(var)) => {
                    let value = &arguments[&var.variable_name];
                    if let Some(candidate) = candidate.as_mut() {
                        candidate.merge(&CandidateValue::Single(value));
                    } else {
                        candidate = Some(CandidateValue::Single(value));
                    }
                }
                Operation::OneOf(_, Argument::Variable(var)) => {
                    let values: Vec<&FieldValue> = arguments[&var.variable_name]
                        .as_vec()
                        .expect("OneOf operation using a non-vec FieldValue")
                        .iter()
                        .map(AsRef::as_ref)
                        .collect();
                    if let Some(candidate) = candidate.as_mut() {
                        candidate.merge(&CandidateValue::Multiple(values));
                    } else {
                        candidate = Some(CandidateValue::Multiple(values));
                    }
                }
                _ => {}
            }
        }

        candidate
    }

    fn static_field_range(&self, field_name: &str) -> Option<&RangeBoundKind> {
        todo!()
    }

    fn dynamic_field_value(&self, field_name: &str) -> Option<DynamicallyResolved<FieldValue>> {
        todo!()
    }

    fn dynamic_field_range(&self, field_name: &str) -> Option<DynamicallyResolved<RangeBoundKind>>;

    // non-optional, non-recursed, non-folded edge
    // TODO: What happens if the same edge exists more than once in a given scope?
    fn required_edge(&self, edge_name: &str) -> Option<EdgeInfo>;

    // optional, recursed, or folded edge;
    // recursed because recursion always starts at depth 0
    // TODO: What happens if the same edge exists more than once in a given scope?
    // fn optional_edge(&self, edge_name: &str) -> Option<EdgeInfo>;
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QueryInfo {
    query: InterpretedQuery,
    current_vertex: Vid,
}

impl QueryInfo {
    pub fn here(&self) -> LocalQueryInfo {
        LocalQueryInfo {
            query: self.query.clone(),
            current_vertex: self.current_vertex,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LocalQueryInfo {
    query: InterpretedQuery,
    current_vertex: Vid,
}

impl VertexInfo for LocalQueryInfo {
    #[inline]
    fn current_vertex(&self) -> &IRVertex {
        &self.current_component().vertices[&self.current_vertex]
    }

    #[inline]
    fn current_component(&self) -> &IRQueryComponent {
        &self.query.indexed_query.vids[&self.current_vertex]
    }

    #[inline]
    fn query_arguments(&self) -> &BTreeMap<Arc<str>, FieldValue> {
        &self.query.arguments
    }

    fn dynamic_field_value(&self, field_name: &str) -> Option<DynamicallyResolved<FieldValue>> {
        todo!()
    }

    fn dynamic_field_range(&self, field_name: &str) -> Option<DynamicallyResolved<RangeBoundKind>> {
        todo!()
    }

    // non-optional, non-recursed, non-folded edge
    fn required_edge(&self, edge_name: &str) -> Option<EdgeInfo> {
        // ensure that calling `dynamic_*()` methods on the result,
        // then calling `resolve()` on *their* result,
        // is aware that the input contexts may need to resolve neighbors first!
        //
        // TODO: What happens if the same edge exists more than once in a given scope?
        todo!()
    }
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct EdgeInfo {
    eid: Eid,
    optional: bool,
    recursive: Option<usize>, // TODO: perhaps a better inner type, to represent "no-limit" too?
    folded: bool,
    destination: NeighboringQueryInfo,
}

impl EdgeInfo {
    pub fn destination(&self) -> &NeighboringQueryInfo {
        &self.destination
    }
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct NeighboringQueryInfo {
    query: InterpretedQuery,
    starting_vertex: Vid,
    neighbor_vertex: Vid,
    neighbor_path: Vec<Eid>,
}

impl VertexInfo for NeighboringQueryInfo {
    #[inline]
    fn current_vertex(&self) -> &IRVertex {
        &self.current_component().vertices[&self.neighbor_vertex]
    }

    #[inline]
    fn current_component(&self) -> &IRQueryComponent {
        &self.query.indexed_query.vids[&self.neighbor_vertex]
    }

    #[inline]
    fn query_arguments(&self) -> &BTreeMap<Arc<str>, FieldValue> {
        &self.query.arguments
    }

    fn dynamic_field_value(&self, field_name: &str) -> Option<DynamicallyResolved<FieldValue>> {
        todo!()
    }

    fn dynamic_field_range(&self, field_name: &str) -> Option<DynamicallyResolved<RangeBoundKind>> {
        todo!()
    }

    fn required_edge(&self, edge_name: &str) -> Option<EdgeInfo> {
        // ensure that `dynamic_*` methods on the result,
        // then calling `resolve()` on *their* result,
        // is aware that the input contexts may need to resolve neighbors first!
        //
        // TODO: What happens if the same edge exists more than once in a given scope?
        todo!()
    }

    // fn optional_edge(&self, edge_name: &str) -> Option<EdgeInfo> {
    //     // similar concerns as above in `required_edge()`
    //     todo!()
    // }
}

#[non_exhaustive]
pub struct DynamicallyResolved<ValueT> {
    context_field: ContextField,
    _marker: PhantomData<ValueT>,
}

impl<ValueT> DynamicallyResolved<ValueT> {
    pub fn resolve<'vertex, VertexT: Debug + Clone + 'vertex, AdapterT: Adapter<'vertex, DataToken=VertexT>>(
        self,
        adapter: &mut AdapterT,
        contexts: ContextIterator<'vertex, VertexT>,
    ) -> ContextOutcomeIterator<'vertex, VertexT, ValueT> {
        todo!()
    }
}
