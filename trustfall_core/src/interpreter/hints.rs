#![allow(unused_variables, dead_code, unreachable_code)]

use std::sync::Arc;
use std::{fmt::Debug, marker::PhantomData};

use crate::{
    interpreter::basic_adapter::{ContextIterator, ContextOutcomeIterator},
    ir::{Eid, FieldValue, IRQuery, IRQueryComponent, Vid},
};

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QueryInfo {
    query: Arc<IRQuery>,
    current_vertex: Vid,
}

impl QueryInfo {
    pub fn here(&self) -> LocalQueryInfo {
        LocalQueryInfo {
            query: self.query.clone(),
            current_component: todo!(),
            current_vertex: self.current_vertex,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LocalQueryInfo {
    query: Arc<IRQuery>,
    current_component: Arc<IRQueryComponent>,
    current_vertex: Vid,
}

impl LocalQueryInfo {
    pub fn coerced_to_type(&self) -> Option<&Arc<str>> {
        todo!()
    }

    pub fn static_field_value(&self, field_name: &str) -> Option<&FieldValue> {
        todo!()
    }

    pub fn dynamic_field_value(&self, field_name: &str) -> Option<DynamicallyResolved<FieldValue>> {
        todo!()
    }

    pub fn static_field_range(&self, field_name: &str) -> Option<&RangeBoundKind> {
        todo!()
    }

    pub fn dynamic_field_range(
        &self,
        field_name: &str,
    ) -> Option<DynamicallyResolved<RangeBoundKind>> {
        todo!()
    }

    // non-optional, non-recursed, non-folded edge
    pub fn required_edge(&self, edge_name: &str) -> Option<EdgeInfo> {
        // ensure that calling `dynamic_*()` methods on the result,
        // then calling `resolve()` on *their* result,
        // is aware that the input contexts may need to resolve neighbors first!
        //
        // TODO: What happens if the same edge exists more than once in a given scope?
        todo!()
    }

    // optional, recursed, or folded edge;
    // recursed because recursion always starts at depth 0
    pub fn optional_edge(&self, edge_name: &str) -> Option<EdgeInfo> {
        // similar concerns as above in `required_edge()`
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
    query: Arc<IRQuery>,
    local_component: Arc<IRQueryComponent>,
    local_vertex: Vid,
    neighbor_component: Arc<IRQueryComponent>,
    neighbor_vertex: Vid,
    neighbor_path: Vec<Eid>,
}

impl NeighboringQueryInfo {
    pub fn coerced_to_type(&self) -> Option<&Arc<str>> {
        todo!()
    }

    pub fn static_required_field_value(&self, field_name: &str) -> Option<&FieldValue> {
        todo!()
    }

    pub fn dynamic_required_field_value(
        &self,
        field_name: &str,
    ) -> Option<DynamicallyResolved<FieldValue>> {
        todo!()
    }

    pub fn static_required_field_range(&self, field_name: &str) -> Option<&RangeBoundKind> {
        todo!()
    }

    pub fn dynamic_required_field_range(
        &self,
        field_name: &str,
    ) -> Option<DynamicallyResolved<RangeBoundKind>> {
        todo!()
    }

    pub fn required_edge(&self, edge_name: &str) -> Option<EdgeInfo> {
        // ensure that `dynamic_*` methods on the result,
        // then calling `resolve()` on *their* result,
        // is aware that the input contexts may need to resolve neighbors first!
        //
        // TODO: What happens if the same edge exists more than once in a given scope?
        todo!()
    }

    pub fn optional_edge(&self, edge_name: &str) -> Option<EdgeInfo> {
        // similar concerns as above in `required_edge()`
        todo!()
    }
}

#[non_exhaustive]
pub struct DynamicallyResolved<ValueT> {
    _marker: PhantomData<ValueT>,
}

impl<ValueT> DynamicallyResolved<ValueT> {
    pub fn resolve<'vertex, VertexT: Debug + Clone + 'vertex>(
        self,
        contexts: ContextIterator<'vertex, VertexT>,
    ) -> ContextOutcomeIterator<'vertex, VertexT, ValueT> {
        todo!()
    }
}

// TODO: should we just use Rust's built in range types?
//       is there an advantage one way or the other?
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeBoundKind {
    Lower(RangeEndpoint),
    Upper(RangeEndpoint),
    LowerAndUpper(RangeEndpoint, RangeEndpoint),
}

impl RangeBoundKind {
    pub fn start(&self) -> Option<&RangeEndpoint> {
        match self {
            RangeBoundKind::Lower(l) => Some(l),
            RangeBoundKind::Upper(_) => None,
            RangeBoundKind::LowerAndUpper(l, _) => Some(l),
        }
    }

    pub fn end(&self) -> Option<&RangeEndpoint> {
        match self {
            RangeBoundKind::Lower(_) => None,
            RangeBoundKind::Upper(r) => Some(r),
            RangeBoundKind::LowerAndUpper(_, r) => Some(r),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeEndpoint {
    Exclusive(FieldValue),
    Inclusive(FieldValue),
}
