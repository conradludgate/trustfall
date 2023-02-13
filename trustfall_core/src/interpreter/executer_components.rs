use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::Arc,
};

use regex::Regex;

use crate::{
    interpreter::filtering::{
        contains, equals, greater_than, greater_than_or_equal, has_prefix, has_substring,
        has_suffix, less_than, less_than_or_equal, one_of, regex_matches_optimized,
        regex_matches_slow_path,
    },
    ir::{Eid, FieldValue, FoldSpecificFieldKind, Operation, Vid},
    util::BTreeMapTryInsertExt,
};

use super::{query_plan::SimpleArgument, DataContext, FieldRef, InterpretedQuery, ValueOrVec};

pub fn collect_outputs<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
    outputs: Vec<Arc<str>>,
) -> impl Iterator<Item = BTreeMap<Arc<str>, FieldValue>> + 'query {
    i.map(move |mut ctx| {
        let mut output: BTreeMap<Arc<str>, FieldValue> =
            outputs.iter().cloned().zip(ctx.values.drain(..)).collect();

        for ((_, output_name), output_value) in ctx.folded_values {
            output.insert(output_name, output_value.into());
        }

        output
    })
}

pub fn push_value<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
    value: FieldValue,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |mut ctx| {
        ctx.values.push(value.to_owned());
        ctx
    })
}

pub fn import_tag<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
    value: FieldRef,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |mut ctx| {
        let value = ctx.imported_tags[&value].clone();
        ctx.values.push(value);
        ctx
    })
}

pub fn record<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
    vid: Vid,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |mut ctx| {
        ctx.record_token(vid);
        ctx
    })
}

pub fn activate<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
    vid: Vid,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |ctx| ctx.activate_token(&vid))
}

pub fn unsuspend<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |ctx| ctx.unsuspend())
}

pub fn suspend_none<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(|mut ctx| {
        if ctx.current_token.is_none() {
            ctx.suspended_tokens.push(None);
        }
        ctx
    })
}

pub fn suspend<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |ctx| ctx.ensure_suspended())
}

pub fn pop_into_import<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
    field: FieldRef,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |mut ctx| {
        ctx.imported_tags.insert(
            field.clone(),
            ctx.values
                .pop()
                .expect("fold-specific field computed and pushed onto the stack"),
        );
        ctx
    })
}

pub fn save_property_value<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = (DataContext<DataToken>, FieldValue)> + 'query,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(|(mut context, value)| {
        context.values.push(value);
        context
    })
}

pub fn coerce_filter<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = (DataContext<DataToken>, bool)> + 'query,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.filter_map(|(ctx, can_coerce)| can_coerce.then_some(ctx))
}

pub fn coerce_suspend<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = (DataContext<DataToken>, bool)> + 'query,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(|(ctx, can_coerce)| {
        if can_coerce {
            ctx
        } else {
            ctx.ensure_suspended()
        }
    })
}

pub fn fold_count<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = DataContext<DataToken>> + 'query,
    fold_eid: Eid,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    i.map(move |mut ctx| {
        let value = ctx.folded_contexts[&fold_eid].len();
        ctx.values.push(FieldValue::Uint64(value as u64));
        ctx
    })
}

pub fn expand_neighbors_required<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = (DataContext<DataToken>, impl Iterator<Item = DataToken>)>,
) -> impl Iterator<Item = DataContext<DataToken>> {
    i.flat_map(move |(context, neighbor_iterator)| {
        EdgeExpander::new(context, neighbor_iterator, false)
    })
}
pub fn expand_neighbors_optional<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = (DataContext<DataToken>, impl Iterator<Item = DataToken>)>,
) -> impl Iterator<Item = DataContext<DataToken>> {
    i.flat_map(move |(context, neighbor_iterator)| {
        EdgeExpander::new(context, neighbor_iterator, true)
    })
}
pub fn expand_neighbors_recurse<'query, DataToken: Clone + Debug + 'query>(
    i: impl Iterator<Item = (DataContext<DataToken>, impl Iterator<Item = DataToken>)>,
) -> impl Iterator<Item = DataContext<DataToken>> {
    i.flat_map(move |(context, neighbor_iterator)| {
        RecursiveEdgeExpander::new(context, neighbor_iterator)
    })
}

/// If this IRFold has a filter on the folded element count, and that filter imposes
/// a max size that can be statically determined, return that max size so it can
/// be used for further optimizations. Otherwise, return None.
pub fn get_max_fold_count_limit(
    query: &InterpretedQuery,
    post_fold_filters: &[Operation<FoldSpecificFieldKind, SimpleArgument>],
) -> Option<usize> {
    let mut result: Option<usize> = None;

    for post_fold_filter in post_fold_filters {
        let next_limit = match post_fold_filter {
            Operation::Equals(FoldSpecificFieldKind::Count, SimpleArgument::Variable(var_ref))
            | Operation::LessThanOrEqual(
                FoldSpecificFieldKind::Count,
                SimpleArgument::Variable(var_ref),
            ) => {
                let variable_value = query.arguments[var_ref.as_ref()].as_usize().unwrap();
                Some(variable_value)
            }
            Operation::LessThan(
                FoldSpecificFieldKind::Count,
                SimpleArgument::Variable(var_ref),
            ) => {
                let variable_value = query.arguments[var_ref.as_ref()].as_usize().unwrap();
                // saturating_sub() here is a safeguard against underflow: in principle,
                // we shouldn't see a comparison for "< 0", but if we do regardless, we'd prefer to
                // saturate to 0 rather than wrapping around. This check is an optimization and
                // is allowed to be more conservative than strictly necessary.
                // The later full application of filters ensures correctness.
                Some(variable_value.saturating_sub(1))
            }
            Operation::OneOf(FoldSpecificFieldKind::Count, SimpleArgument::Variable(var_ref)) => {
                match &query.arguments[var_ref.as_ref()] {
                    FieldValue::List(v) => v.iter().map(|x| x.as_usize().unwrap()).max(),
                    _ => unreachable!(),
                }
            }
            _ => None,
        };

        match (result, next_limit) {
            (None, _) => result = next_limit,
            (Some(l), Some(r)) if l > r => result = next_limit,
            _ => {}
        }
    }

    result
}

pub fn neighbors_import_tags<DataToken: Clone + Debug>(
    i: impl Iterator<Item = DataToken>,
    context: &DataContext<DataToken>,
) -> impl Iterator<Item = DataContext<DataToken>> {
    let imported_tags = context.imported_tags.clone();
    i.map(move |x| {
        let mut ctx = DataContext::new(x);
        ctx.imported_tags = imported_tags.clone();
        ctx
    })
}

pub fn store_folded<DataToken: Clone + Debug>(
    context: &mut DataContext<DataToken>,
    eid: Eid,
    fold_elements: Vec<DataContext<DataToken>>,
    tags: &[FieldRef],
) {
    context
        .folded_contexts
        .insert_or_error(eid, fold_elements)
        .unwrap();

    // Remove no-longer-needed imported tags.
    for imported_tag in tags {
        context.imported_tags.remove(imported_tag).unwrap();
    }
}

pub fn extract_folded_context<DataToken: Clone + Debug>(
    ctx: &mut DataContext<DataToken>,
    eid: Eid,
    vid: Vid,
    fold_specific_outputs: &BTreeMap<Arc<str>, FoldSpecificFieldKind>,
    output_names: &[Arc<str>],
    folded_keys: &[(Eid, Arc<str>)],
) -> Option<(
    BTreeMap<(Eid, Arc<str>), Option<ValueOrVec>>,
    Vec<DataContext<DataToken>>,
)> {
    // If the @fold is inside an @optional that doesn't exist,
    // its outputs should be `null` rather than empty lists (the usual for empty folds).
    // Transformed outputs should also be `null` rather than their usual transformed defaults.
    let did_fold_root_exist = ctx.tokens[&vid].is_some();
    let default_value = did_fold_root_exist.then_some(ValueOrVec::Vec(Vec::new()));

    let fold_elements = ctx.folded_contexts.get(&eid).unwrap();

    // Add any fold-specific field outputs to the context's folded values.
    for (output_name, fold_specific_field) in fold_specific_outputs {
        let value = match fold_specific_field {
            FoldSpecificFieldKind::Count => {
                ValueOrVec::Value(FieldValue::Uint64(fold_elements.len() as u64))
            }
        };
        ctx.folded_values
            .insert_or_error(
                (eid, output_name.clone()),
                did_fold_root_exist.then_some(value),
            )
            .unwrap();
    }

    // Prepare empty vectors for all the outputs from this @fold component.
    // If the fold-root vertex didn't exist, the default is `null` instead.
    let mut folded_values: BTreeMap<(Eid, Arc<str>), Option<ValueOrVec>> = output_names
        .iter()
        .map(|output| ((eid, output.clone()), default_value.clone()))
        .collect();

    if fold_elements.is_empty() {
        // We need to make sure any outputs from any nested @fold components (recursively)
        // are set to empty lists.
        for key in folded_keys {
            folded_values.insert(key.clone(), default_value.clone());
        }
        ctx.folded_values.extend(folded_values);
        None
    } else {
        Some((folded_values, fold_elements.clone()))
    }
}

pub fn store_folded_values<DataToken: Clone + Debug>(
    i: impl Iterator<Item = DataContext<DataToken>>,
    ctx: &mut DataContext<DataToken>,
    mut folded_values: BTreeMap<(Eid, Arc<str>), Option<ValueOrVec>>,
    output_names: &[Arc<str>],
    eid: Eid,
) {
    for mut folded_context in i {
        for (key, value) in folded_context.folded_values {
            folded_values
                .entry(key)
                .or_insert_with(|| Some(ValueOrVec::Vec(vec![])))
                .as_mut()
                .expect("not Some")
                .as_mut_vec()
                .expect("not a Vec")
                .push(value.unwrap_or(ValueOrVec::Value(FieldValue::Null)));
        }

        // We pushed values onto folded_context.values with output names in increasing order
        // and we are now popping from the back. That means we're getting the highest name
        // first, so we should reverse our output_names iteration order.
        for output in output_names.iter().rev() {
            let value = folded_context.values.pop().unwrap();
            folded_values
                .get_mut(&(eid, output.clone()))
                .expect("key not present")
                .as_mut()
                .expect("value was None")
                .as_mut_vec()
                .expect("not a Vec")
                .push(ValueOrVec::Value(value));
        }
    }
    ctx.folded_values.extend(folded_values);
}

pub fn collect_fold_elements<'query, DataToken: Clone + Debug + 'query>(
    mut iterator: impl Iterator<Item = DataContext<DataToken>> + 'query,
    max_fold_count_limit: usize,
) -> Option<Vec<DataContext<DataToken>>> {
    // If this fold has more than `max_fold_count_limit` elements,
    // it will get filtered out by a post-fold filter.
    // Pulling elements from `iterator` causes computations and data fetches to happen,
    // and as an optimization we'd like to stop pulling elements as soon as possible.
    // If we are able to pull more than `max_fold_count_limit + 1` elements,
    // we know that this fold is going to get filtered out, so we might as well
    // stop materializing its elements early.
    let mut fold_elements = Vec::with_capacity(max_fold_count_limit);

    let mut stopped_early = false;
    for _ in 0..max_fold_count_limit {
        if let Some(element) = iterator.next() {
            fold_elements.push(element);
        } else {
            stopped_early = true;
            break;
        }
    }

    if !stopped_early && iterator.next().is_some() {
        // There are more elements than the max size allowed by the filters on this fold.
        // It's going to get filtered out anyway, so we can avoid materializing the rest.
        return None;
    }

    Some(fold_elements)
}

/// Check whether a tagged value that is being used in a filter originates from
/// a scope that is optional and missing, and therefore the filter should pass.
///
/// A small subtlety is important here: it's possible that the tagged value is *local* to
/// the scope being filtered. In that case, the context *will not* yet have a token associated
/// with the [Vid] of the tag's ContextField. However, in such cases, the tagged value
/// is *never* optional relative to the current scope, so we can safely return `false`.
#[inline(always)]
fn is_tag_optional_and_missing<'query, DataToken: Clone + Debug + 'query>(
    context: &DataContext<DataToken>,
    vid: Vid,
) -> bool {
    // Some(None) means "there's a value associated with that Vid, and it's None".
    // None would mean that the tagged value is local, i.e. nothing is associated with that Vid yet.
    // Some(Some(token)) would mean that a vertex was found and associated with that Vid.
    matches!(context.tokens.get(&vid), Some(None))
}

fn filter_map<'query, DataToken: Clone + Debug + 'query>(
    expression_iterator: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
    right: SimpleArgument,
    mut f: impl for<'a> FnMut(&'a FieldValue, &'a FieldValue) -> bool + 'query,
) -> Box<dyn Iterator<Item = DataContext<DataToken>> + 'query> {
    Box::new(expression_iterator.filter_map(move |mut context| {
        let right_value = context.values.pop().unwrap();
        let left_value = context.values.pop().unwrap();
        if let SimpleArgument::Tag(vid) = &right {
            if is_tag_optional_and_missing(&context, *vid) {
                return Some(context);
            }
        }
        f(&left_value, &right_value).then_some(context)
    }))
}

fn not<'query>(
    mut f: impl for<'a> FnMut(&'a FieldValue, &'a FieldValue) -> bool + 'query,
) -> impl for<'a> FnMut(&'a FieldValue, &'a FieldValue) -> bool + 'query {
    move |l, r| !f(l, r)
}

fn apply_filter<'query, DataToken: Clone + Debug + 'query>(
    query: &InterpretedQuery,
    filter: Operation<(), SimpleArgument>,
    expression_iterator: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
) -> Box<dyn Iterator<Item = DataContext<DataToken>> + 'query> {
    match filter {
        Operation::IsNull(_) => {
            let output_iter = expression_iterator.filter_map(move |mut context| {
                let last_value = context.values.pop().unwrap();
                match last_value {
                    FieldValue::Null => Some(context),
                    _ => None,
                }
            });
            Box::new(output_iter)
        }
        Operation::IsNotNull(_) => {
            let output_iter = expression_iterator.filter_map(move |mut context| {
                let last_value = context.values.pop().unwrap();
                match last_value {
                    FieldValue::Null => None,
                    _ => Some(context),
                }
            });
            Box::new(output_iter)
        }
        Operation::Equals(_, right) => filter_map(expression_iterator, right, equals),
        Operation::NotEquals(_, right) => filter_map(expression_iterator, right, not(equals)),
        Operation::GreaterThan(_, right) => filter_map(expression_iterator, right, greater_than),
        Operation::GreaterThanOrEqual(_, right) => {
            filter_map(expression_iterator, right, greater_than_or_equal)
        }
        Operation::LessThan(_, right) => filter_map(expression_iterator, right, less_than),
        Operation::LessThanOrEqual(_, right) => {
            filter_map(expression_iterator, right, less_than_or_equal)
        }
        Operation::HasSubstring(_, right) => filter_map(expression_iterator, right, has_substring),
        Operation::NotHasSubstring(_, right) => {
            filter_map(expression_iterator, right, not(has_substring))
        }
        Operation::OneOf(_, right) => filter_map(expression_iterator, right, one_of),
        Operation::NotOneOf(_, right) => filter_map(expression_iterator, right, not(one_of)),
        Operation::Contains(_, right) => filter_map(expression_iterator, right, contains),
        Operation::NotContains(_, right) => filter_map(expression_iterator, right, not(contains)),
        Operation::HasPrefix(_, right) => filter_map(expression_iterator, right, has_prefix),
        Operation::NotHasPrefix(_, right) => {
            filter_map(expression_iterator, right, not(has_prefix))
        }
        Operation::HasSuffix(_, right) => filter_map(expression_iterator, right, has_suffix),
        Operation::NotHasSuffix(_, right) => {
            filter_map(expression_iterator, right, not(has_suffix))
        }
        Operation::RegexMatches(_, right) => match &right {
            SimpleArgument::Tag(_) => {
                filter_map(expression_iterator, right, regex_matches_slow_path)
            }
            SimpleArgument::Variable(var) => {
                let variable_value = &query.arguments[var.as_ref()];
                let pattern = Regex::new(variable_value.as_str().unwrap()).unwrap();

                Box::new(expression_iterator.filter_map(move |mut context| {
                    let _ = context.values.pop().unwrap();
                    let left_value = context.values.pop().unwrap();

                    if regex_matches_optimized(&left_value, &pattern) {
                        Some(context)
                    } else {
                        None
                    }
                }))
            }
        },
        Operation::NotRegexMatches(_, right) => match &right {
            SimpleArgument::Tag(_) => {
                filter_map(expression_iterator, right, not(regex_matches_slow_path))
            }
            SimpleArgument::Variable(var) => {
                let variable_value = &query.arguments[var.as_ref()];
                let pattern = Regex::new(variable_value.as_str().unwrap()).unwrap();

                Box::new(expression_iterator.filter_map(move |mut context| {
                    let _ = context.values.pop().unwrap();
                    let left_value = context.values.pop().unwrap();

                    if !regex_matches_optimized(&left_value, &pattern) {
                        Some(context)
                    } else {
                        None
                    }
                }))
            }
        },
    }
}
struct EdgeExpander<I: Iterator>
where
    I::Item: Clone + Debug,
{
    context: DataContext<I::Item>,
    neighbor_tokens: Option<I>,
    is_optional_edge: bool,
    has_neighbors: bool,
}

impl<I: Iterator> EdgeExpander<I>
where
    I::Item: Clone + Debug,
{
    pub fn new(context: DataContext<I::Item>, neighbor_tokens: I, is_optional_edge: bool) -> Self {
        EdgeExpander {
            context,
            neighbor_tokens: Some(neighbor_tokens),
            is_optional_edge,
            has_neighbors: false,
        }
    }
}

impl<I: Iterator> Iterator for EdgeExpander<I>
where
    I::Item: Clone + Debug,
{
    type Item = DataContext<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let neighbor_tokens = self.neighbor_tokens.as_mut()?;

        let neighbor = neighbor_tokens.next();
        if neighbor.is_some() {
            self.has_neighbors = true;
            return Some(self.context.split_and_move_to_token(neighbor));
        } else {
            self.neighbor_tokens = None;
        }

        if self.context.current_token.is_none() {
            // If there's no current token, there couldn't possibly be neighbors.
            // If this assertion trips, the adapter's project_neighbors() implementation illegally
            // returned neighbors for a non-existent vertex.
            assert!(!self.has_neighbors);
            Some(self.context.split_and_move_to_token(None))
        } else if self.is_optional_edge && !self.has_neighbors {
            // The edge is optional and we havent returned anything, so
            // return the empty context
            Some(self.context.split_and_move_to_token(None))
        } else {
            None
        }
    }
}

struct RecursiveEdgeExpander<I: Iterator>
where
    I::Item: Clone + Debug,
{
    context: Option<DataContext<I::Item>>,
    neighbor_base: Option<DataContext<I::Item>>,
    neighbor_tokens: Option<I>,
}

impl<I: Iterator> RecursiveEdgeExpander<I>
where
    I::Item: Clone + Debug,
{
    pub fn new(context: DataContext<I::Item>, neighbor_tokens: I) -> RecursiveEdgeExpander<I> {
        RecursiveEdgeExpander {
            context: Some(context),
            neighbor_base: None,
            neighbor_tokens: Some(neighbor_tokens),
        }
    }
}

impl<I: Iterator> Iterator for RecursiveEdgeExpander<I>
where
    I::Item: Clone + Debug,
{
    type Item = DataContext<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(neighbor_tokens) = self.neighbor_tokens.as_mut() {
            let neighbor = neighbor_tokens.next();

            if let Some(token) = neighbor {
                if let Some(context) = self.context.take() {
                    // Prep a neighbor base context for future use, since we're moving
                    // the "self" context out.
                    self.neighbor_base = Some(context.split_and_move_to_token(None));

                    // Attach the "self" context as a piggyback rider on the neighbor.
                    let mut neighbor_context = context.split_and_move_to_token(Some(token));
                    neighbor_context
                        .piggyback
                        .get_or_insert_with(Default::default)
                        .push(context.ensure_suspended());
                    return Some(neighbor_context);
                } else {
                    // The "self" token has already been moved out, so use the neighbor base context
                    // as the starting point for constructing a new context.
                    return Some(
                        self.neighbor_base
                            .as_ref()
                            .unwrap()
                            .split_and_move_to_token(Some(token)),
                    );
                }
            } else {
                self.neighbor_tokens = None;
            }
        }

        self.context.take()
    }
}

fn unpack_piggyback<DataToken: Debug + Clone>(
    context: DataContext<DataToken>,
) -> Vec<DataContext<DataToken>> {
    let mut result = Default::default();

    unpack_piggyback_inner(&mut result, context);

    result
}

fn unpack_piggyback_inner<DataToken: Debug + Clone>(
    output: &mut Vec<DataContext<DataToken>>,
    mut context: DataContext<DataToken>,
) {
    if let Some(piggyback) = context.piggyback.take() {
        for ctx in piggyback {
            unpack_piggyback_inner(output, ctx);
        }
    }

    output.push(context);
}

pub fn post_process_recursive_expansion<'query, DataToken: Clone + Debug + 'query>(
    iterator: impl Iterator<Item = DataContext<DataToken>> + 'query,
) -> impl Iterator<Item = DataContext<DataToken>> + 'query {
    iterator
        .flat_map(|context| unpack_piggyback(context))
        .map(|context| {
            assert!(context.piggyback.is_none());
            context.ensure_unsuspended()
        })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use trustfall_filetests_macros::parameterize;

    use crate::{
        interpreter::{error::QueryArgumentsError, InterpretedQuery},
        ir::{indexed::IndexedQuery, FieldValue},
        util::TestIRQueryResult,
    };

    #[parameterize("trustfall_core/src/resources/test_data/execution_errors")]
    fn parameterizable_tester(base: &Path, stem: &str) {
        let mut input_path = PathBuf::from(base);
        input_path.push(format!("{stem}.ir.ron"));

        let mut check_path = PathBuf::from(base);
        check_path.push(format!("{stem}.exec-error.ron"));
        let check_data = fs::read_to_string(check_path).unwrap();

        let input_data = fs::read_to_string(input_path).unwrap();
        let test_query: TestIRQueryResult = ron::from_str(&input_data).unwrap();
        let test_query = test_query.unwrap();

        let arguments: BTreeMap<Arc<str>, FieldValue> = test_query
            .arguments
            .into_iter()
            .map(|(k, v)| (Arc::from(k), v))
            .collect();

        let indexed_query: IndexedQuery = test_query.ir_query.try_into().unwrap();
        let constructed_test_item = InterpretedQuery::from_query_and_arguments(
            &indexed_query.ir_query.variables,
            Arc::from(arguments),
        );

        let check_parsed: Result<_, QueryArgumentsError> = Err(ron::from_str(&check_data).unwrap());

        assert_eq!(check_parsed, constructed_test_item);
    }
}
