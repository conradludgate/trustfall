use std::{
    cell::RefCell,
    collections::{btree_map, BTreeMap, VecDeque},
    convert::TryInto,
    fmt::Debug,
    marker::PhantomData,
    rc::Rc,
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::ir::{indexed::IndexedQuery, EdgeParameters, Eid, FieldValue, Vid};

use super::{
    execution::interpret_ir,
    trace::{FunctionCall, Opid, Trace, TraceOp, TraceOpContent, YieldValue},
    Adapter, DataContext, InterpretedQuery,
};

#[derive(Clone, Debug)]
struct TraceReaderAdapter<'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'trace,
    for<'de2> DataToken: Deserialize<'de2>,
{
    next_op: Rc<RefCell<btree_map::Iter<'trace, Opid, TraceOp<DataToken>>>>,
}

fn advance_ref_iter<T, Iter: Iterator<Item = T>>(iter: &RefCell<Iter>) -> Option<T> {
    // We do this through a separate function to ensure the mut borrow is dropped
    // as early as possible, to avoid overlapping mut borrows.
    iter.borrow_mut().next()
}

#[derive(Debug)]
struct TraceReaderStartingTokensIter<'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'trace,
    for<'de2> DataToken: Deserialize<'de2>,
{
    exhausted: bool,
    parent_opid: Opid,
    inner: Rc<RefCell<btree_map::Iter<'trace, Opid, TraceOp<DataToken>>>>,
}

#[allow(unused_variables)]
impl<'trace, DataToken> Iterator for TraceReaderStartingTokensIter<'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'trace,
    for<'de2> DataToken: Deserialize<'de2>,
{
    type Item = DataToken;

    fn next(&mut self) -> Option<Self::Item> {
        assert!(!self.exhausted);

        let (_, trace_op) = advance_ref_iter(self.inner.as_ref())
            .expect("Expected to have an item but found none.");
        assert_eq!(
            self.parent_opid,
            trace_op
                .parent_opid
                .expect("Expected an operation with a parent_opid."),
            "Expected parent_opid {:?} did not match operation {:#?}",
            self.parent_opid,
            trace_op,
        );

        match &trace_op.content {
            TraceOpContent::OutputIteratorExhausted => {
                self.exhausted = true;
                None
            }
            TraceOpContent::YieldFrom(YieldValue::GetStartingTokens(token)) => Some(token.clone()),
            _ => unreachable!(),
        }
    }
}

struct TraceReaderProjectPropertiesIter<'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'trace,
    for<'de2> DataToken: Deserialize<'de2>,
{
    exhausted: bool,
    parent_opid: Opid,
    data_contexts: Box<dyn Iterator<Item = DataContext<DataToken>> + 'trace>,
    input_batch: VecDeque<DataContext<DataToken>>,
    inner: Rc<RefCell<btree_map::Iter<'trace, Opid, TraceOp<DataToken>>>>,
}

#[allow(unused_variables)]
impl<'trace, DataToken> Iterator for TraceReaderProjectPropertiesIter<'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'trace,
    for<'de2> DataToken: Deserialize<'de2>,
{
    type Item = (DataContext<DataToken>, FieldValue);

    fn next(&mut self) -> Option<Self::Item> {
        assert!(!self.exhausted);
        let next_op = loop {
            let (_, input_op) = advance_ref_iter(self.inner.as_ref())
                .expect("Expected to have an item but found none.");
            assert_eq!(
                self.parent_opid,
                input_op
                    .parent_opid
                    .expect("Expected an operation with a parent_opid."),
                "Expected parent_opid {:?} did not match operation {:#?}",
                self.parent_opid,
                input_op,
            );

            if let TraceOpContent::AdvanceInputIterator = &input_op.content {
                let input_data = self.data_contexts.next();

                let (_, input_op) = advance_ref_iter(self.inner.as_ref())
                    .expect("Expected to have an item but found none.");
                assert_eq!(
                    self.parent_opid,
                    input_op
                        .parent_opid
                        .expect("Expected an operation with a parent_opid."),
                    "Expected parent_opid {:?} did not match operation {:#?}",
                    self.parent_opid,
                    input_op,
                );

                if let TraceOpContent::YieldInto(context) = &input_op.content {
                    let input_context = input_data.unwrap();
                    assert_eq!(context, &input_context);
                    self.input_batch.push_back(input_context);
                } else if let TraceOpContent::InputIteratorExhausted = &input_op.content {
                    assert_eq!(None, input_data);
                } else {
                    unreachable!();
                }
            } else {
                break input_op;
            }
        };

        match &next_op.content {
            TraceOpContent::YieldFrom(YieldValue::ProjectProperty(trace_context, value)) => {
                let input_context = self.input_batch.pop_front().unwrap();
                assert_eq!(trace_context, &input_context);
                Some((input_context, value.clone()))
            }
            TraceOpContent::OutputIteratorExhausted => {
                assert_eq!(None, self.input_batch.pop_front());
                self.exhausted = true;
                None
            }
            _ => unreachable!(),
        }
    }
}

struct TraceReaderCanCoerceIter<'query, 'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'query,
    for<'de2> DataToken: Deserialize<'de2>,
    'trace: 'query,
{
    exhausted: bool,
    parent_opid: Opid,
    data_contexts: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
    input_batch: VecDeque<DataContext<DataToken>>,
    inner: Rc<RefCell<btree_map::Iter<'trace, Opid, TraceOp<DataToken>>>>,
}

#[allow(unused_variables)]
impl<'query, 'trace, DataToken> Iterator for TraceReaderCanCoerceIter<'query, 'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'query,
    for<'de2> DataToken: Deserialize<'de2>,
    'trace: 'query,
{
    type Item = (DataContext<DataToken>, bool);

    fn next(&mut self) -> Option<Self::Item> {
        assert!(!self.exhausted);
        let next_op = loop {
            let (_, input_op) = advance_ref_iter(self.inner.as_ref())
                .expect("Expected to have an item but found none.");
            assert_eq!(
                self.parent_opid,
                input_op
                    .parent_opid
                    .expect("Expected an operation with a parent_opid."),
                "Expected parent_opid {:?} did not match operation {:#?}",
                self.parent_opid,
                input_op,
            );

            if let TraceOpContent::AdvanceInputIterator = &input_op.content {
                let input_data = self.data_contexts.next();

                let (_, input_op) = advance_ref_iter(self.inner.as_ref())
                    .expect("Expected to have an item but found none.");
                assert_eq!(
                    self.parent_opid,
                    input_op
                        .parent_opid
                        .expect("Expected an operation with a parent_opid."),
                    "Expected parent_opid {:?} did not match operation {:#?}",
                    self.parent_opid,
                    input_op,
                );

                if let TraceOpContent::YieldInto(context) = &input_op.content {
                    let input_context = input_data.unwrap();
                    assert_eq!(context, &input_context);

                    self.input_batch.push_back(input_context);
                } else if let TraceOpContent::InputIteratorExhausted = &input_op.content {
                    assert_eq!(None, input_data);
                } else {
                    unreachable!();
                }
            } else {
                break input_op;
            }
        };

        match &next_op.content {
            TraceOpContent::YieldFrom(YieldValue::CanCoerceToType(trace_context, can_coerce)) => {
                let input_context = self.input_batch.pop_front().unwrap();
                assert_eq!(trace_context, &input_context);
                Some((input_context, *can_coerce))
            }
            TraceOpContent::OutputIteratorExhausted => {
                assert_eq!(None, self.input_batch.pop_front());
                self.exhausted = true;
                None
            }
            _ => unreachable!(),
        }
    }
}

struct TraceReaderProjectNeighborsIter<'query, 'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'query,
    for<'de2> DataToken: Deserialize<'de2>,
    'trace: 'query,
{
    exhausted: bool,
    parent_opid: Opid,
    data_contexts: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
    input_batch: VecDeque<DataContext<DataToken>>,
    inner: Rc<RefCell<btree_map::Iter<'trace, Opid, TraceOp<DataToken>>>>,
}

impl<'query, 'trace, DataToken> Iterator
    for TraceReaderProjectNeighborsIter<'query, 'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'query,
    for<'de2> DataToken: Deserialize<'de2>,
    'trace: 'query,
{
    type Item = (
        DataContext<DataToken>,
        Box<dyn Iterator<Item = DataToken> + 'query>,
    );

    fn next(&mut self) -> Option<Self::Item> {
        assert!(!self.exhausted);
        let next_op = loop {
            let (_, input_op) = advance_ref_iter(self.inner.as_ref())
                .expect("Expected to have an item but found none.");
            assert_eq!(
                self.parent_opid,
                input_op
                    .parent_opid
                    .expect("Expected an operation with a parent_opid."),
                "Expected parent_opid {:?} did not match operation {:#?}",
                self.parent_opid,
                input_op,
            );

            if let TraceOpContent::AdvanceInputIterator = &input_op.content {
                let input_data = self.data_contexts.next();

                let (_, input_op) = advance_ref_iter(self.inner.as_ref())
                    .expect("Expected to have an item but found none.");
                assert_eq!(
                    self.parent_opid,
                    input_op
                        .parent_opid
                        .expect("Expected an operation with a parent_opid."),
                    "Expected parent_opid {:?} did not match operation {:#?}",
                    self.parent_opid,
                    input_op,
                );

                if let TraceOpContent::YieldInto(context) = &input_op.content {
                    let input_context = input_data.unwrap();
                    assert_eq!(context, &input_context);

                    self.input_batch.push_back(input_context);
                } else if let TraceOpContent::InputIteratorExhausted = &input_op.content {
                    assert_eq!(None, input_data);
                } else {
                    unreachable!();
                }
            } else {
                break input_op;
            }
        };

        match &next_op.content {
            TraceOpContent::YieldFrom(YieldValue::ProjectNeighborsOuter(trace_context)) => {
                let input_context = self.input_batch.pop_front().unwrap();
                assert_eq!(trace_context, &input_context);

                let neighbors = Box::new(TraceReaderNeighborIter {
                    exhausted: false,
                    parent_iterator_opid: next_op.opid,
                    next_index: 0,
                    inner: self.inner.clone(),
                    _phantom: PhantomData,
                });
                Some((input_context, neighbors))
            }
            TraceOpContent::OutputIteratorExhausted => {
                assert_eq!(None, self.input_batch.pop_front());
                self.exhausted = true;
                None
            }
            _ => unreachable!(),
        }
    }
}

struct TraceReaderNeighborIter<'query, 'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'query,
    for<'de2> DataToken: Deserialize<'de2>,
    'trace: 'query,
{
    exhausted: bool,
    parent_iterator_opid: Opid,
    next_index: usize,
    inner: Rc<RefCell<btree_map::Iter<'trace, Opid, TraceOp<DataToken>>>>,
    _phantom: PhantomData<&'query ()>,
}

impl<'query, 'trace, DataToken> Iterator for TraceReaderNeighborIter<'query, 'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'query,
    for<'de2> DataToken: Deserialize<'de2>,
    'trace: 'query,
{
    type Item = DataToken;

    fn next(&mut self) -> Option<Self::Item> {
        let (_, trace_op) = advance_ref_iter(self.inner.as_ref())
            .expect("Expected to have an item but found none.");
        assert!(!self.exhausted);
        assert_eq!(
            self.parent_iterator_opid,
            trace_op
                .parent_opid
                .expect("Expected an operation with a parent_opid."),
            "Expected parent_opid {:?} did not match operation {:#?}",
            self.parent_iterator_opid,
            trace_op,
        );

        match &trace_op.content {
            TraceOpContent::OutputIteratorExhausted => {
                self.exhausted = true;
                None
            }
            TraceOpContent::YieldFrom(YieldValue::ProjectNeighborsInner(index, token)) => {
                assert_eq!(self.next_index, *index);
                self.next_index += 1;
                Some(token.clone())
            }
            _ => unreachable!(),
        }
    }
}

#[allow(unused_variables)]
impl<'trace, DataToken> Adapter<'trace> for TraceReaderAdapter<'trace, DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'trace,
    for<'de2> DataToken: Deserialize<'de2>,
{
    type DataToken = DataToken;

    fn get_starting_tokens(
        &self,
        edge: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = Self::DataToken> + 'trace> {
        let (root_opid, trace_op) = advance_ref_iter(self.next_op.as_ref())
            .expect("Expected a get_starting_tokens() call operation, but found none.");
        assert_eq!(None, trace_op.parent_opid);

        if let TraceOpContent::Call(FunctionCall::GetStartingTokens(vid)) = trace_op.content {
            assert_eq!(vid, vertex_hint);

            Box::new(TraceReaderStartingTokensIter {
                exhausted: false,
                parent_opid: *root_opid,
                inner: self.next_op.clone(),
            })
        } else {
            unreachable!()
        }
    }

    fn project_property(
        &self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>> + 'trace>,
        current_type_name: Arc<str>,
        field_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = (DataContext<Self::DataToken>, FieldValue)> + 'trace> {
        let (root_opid, trace_op) = advance_ref_iter(self.next_op.as_ref())
            .expect("Expected a project_property() call operation, but found none.");
        assert_eq!(None, trace_op.parent_opid);

        if let TraceOpContent::Call(FunctionCall::ProjectProperty(vid, type_name, property)) =
            &trace_op.content
        {
            assert_eq!(*vid, vertex_hint);
            assert_eq!(*type_name, current_type_name);
            assert_eq!(*property, field_name);

            Box::new(TraceReaderProjectPropertiesIter {
                exhausted: false,
                parent_opid: *root_opid,
                data_contexts,
                input_batch: Default::default(),
                inner: self.next_op.clone(),
            })
        } else {
            unreachable!()
        }
    }

    #[allow(clippy::type_complexity)]
    fn project_neighbors(
        &self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>> + 'trace>,
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
                    Box<dyn Iterator<Item = Self::DataToken> + 'trace>,
                ),
            > + 'trace,
    > {
        let (root_opid, trace_op) = advance_ref_iter(self.next_op.as_ref())
            .expect("Expected a project_property() call operation, but found none.");
        assert_eq!(None, trace_op.parent_opid);

        if let TraceOpContent::Call(FunctionCall::ProjectNeighbors(vid, type_name, eid)) =
            &trace_op.content
        {
            assert_eq!(vid, &vertex_hint);
            assert_eq!(type_name, &current_type_name);
            assert_eq!(eid, &edge_hint);

            Box::new(TraceReaderProjectNeighborsIter {
                exhausted: false,
                parent_opid: *root_opid,
                data_contexts,
                input_batch: Default::default(),
                inner: self.next_op.clone(),
            })
        } else {
            unreachable!()
        }
    }

    fn can_coerce_to_type(
        &self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>> + 'trace>,
        current_type_name: Arc<str>,
        coerce_to_type_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = (DataContext<Self::DataToken>, bool)> + 'trace> {
        let (root_opid, trace_op) = advance_ref_iter(self.next_op.as_ref())
            .expect("Expected a can_coerce_to_type() call operation, but found none.");
        assert_eq!(None, trace_op.parent_opid);

        if let TraceOpContent::Call(FunctionCall::CanCoerceToType(vid, from_type, to_type)) =
            &trace_op.content
        {
            assert_eq!(*vid, vertex_hint);
            assert_eq!(*from_type, current_type_name);
            assert_eq!(*to_type, coerce_to_type_name);

            Box::new(TraceReaderCanCoerceIter {
                exhausted: false,
                parent_opid: *root_opid,
                data_contexts,
                input_batch: Default::default(),
                inner: self.next_op.clone(),
            })
        } else {
            unreachable!()
        }
    }
}

#[allow(dead_code)]
pub fn assert_interpreted_results<'query, 'trace, DataToken>(
    trace: &Trace<DataToken>,
    expected_results: &[BTreeMap<Arc<str>, FieldValue>],
    complete: bool,
) where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize + 'query,
    for<'de2> DataToken: Deserialize<'de2>,
    'trace: 'query,
{
    let next_op = Rc::new(RefCell::new(trace.ops.iter()));
    let trace_reader_adapter = Rc::new(TraceReaderAdapter {
        next_op: next_op.clone(),
    });

    let query: Arc<IndexedQuery> = Arc::new(trace.ir_query.clone().try_into().unwrap());
    let arguments = Arc::new(
        trace
            .arguments
            .iter()
            .map(|(k, v)| (Arc::from(k.to_owned()), v.clone()))
            .collect(),
    );
    let mut trace_iter = interpret_ir(trace_reader_adapter, query, arguments).unwrap();
    let mut expected_iter = expected_results.iter();

    loop {
        let expected_row = expected_iter.next();
        let trace_row = trace_iter.next();

        if let Some(expected_row_content) = expected_row {
            let trace_expected_row = {
                let mut next_op_ref = next_op.borrow_mut();
                let Some((_, trace_op)) = next_op_ref.next() else {
                    panic!("Reached the end of the trace without producing result {trace_row:#?}");
                };
                let TraceOpContent::ProduceQueryResult(expected_result) = &trace_op.content else {
                    panic!("Expected the trace to produce a result {trace_row:#?} but got another type of operation instead: {trace_op:#?}");
                };
                drop(next_op_ref);

                expected_result
            };
            assert_eq!(
                trace_expected_row, expected_row_content,
                "This trace is self-inconsistent: trace produces row {trace_expected_row:#?} \
                but results have row {expected_row_content:#?}",
            );

            assert_eq!(expected_row, trace_row.as_ref());
        } else {
            if complete {
                assert_eq!(None, trace_row);
            }
            return;
        }
    }
}

