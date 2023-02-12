use std::{collections::BTreeMap, fmt::Debug, rc::Rc, sync::Arc};

use dbg_pls::color;
use regex::Regex;

use crate::{
    interpreter::{
        filtering::{
            contains, equals, greater_than, greater_than_or_equal, has_prefix, has_substring,
            has_suffix, less_than, less_than_or_equal, one_of, regex_matches_optimized,
            regex_matches_slow_path,
        },
        ValueOrVec,
    },
    ir::{indexed::IndexedQuery, Eid, FieldValue, FoldSpecificFieldKind, Operation, Vid},
    util::BTreeMapTryInsertExt,
};

use super::{
    error::QueryArgumentsError,
    query_plan::{query_plan, CoerceKind, ExpandKind, QueryPlan, QueryPlanItem, SimpleArgument},
    Adapter, DataContext, InterpretedQuery,
};

#[allow(clippy::type_complexity)]
pub fn interpret_ir<'query, DataToken>(
    adapter: Rc<impl Adapter<'query, DataToken = DataToken> + 'query>,
    indexed_query: Arc<IndexedQuery>,
    arguments: Arc<BTreeMap<Arc<str>, FieldValue>>,
) -> Result<Box<dyn Iterator<Item = BTreeMap<Arc<str>, FieldValue>> + 'query>, QueryArgumentsError>
where
    DataToken: Clone + Debug + 'query,
{
    // color!(indexed_query.clone());
    let plan = query_plan(indexed_query);
    execute_plan(adapter, plan, arguments)
}

#[allow(clippy::type_complexity)]
pub fn execute_plan<'query, DataToken>(
    adapter: Rc<impl Adapter<'query, DataToken = DataToken> + 'query>,
    plan: QueryPlan,
    arguments: Arc<BTreeMap<Arc<str>, FieldValue>>,
) -> Result<Box<dyn Iterator<Item = BTreeMap<Arc<str>, FieldValue>> + 'query>, QueryArgumentsError>
where
    DataToken: Clone + Debug + 'query,
{
    color!(&plan.plan);
    let query = InterpretedQuery::from_query_and_arguments(&plan.variables, arguments)?;

    let iter = {
        Box::new(
            adapter
                .get_starting_tokens(plan.edge, plan.params, query.clone(), plan.vid)
                .map(|x| DataContext::new(x)),
        )
    };

    let iter = build_plan(adapter, &query, plan.plan, iter);

    let iter = Box::new(iter.map(move |mut context| {
        let mut output: BTreeMap<Arc<str>, FieldValue> = plan
            .outputs
            .iter()
            .cloned()
            .zip(context.values.drain(..))
            .collect();

        for ((_, output_name), output_value) in context.folded_values {
            output.insert(output_name, output_value.into());
        }

        output
    }));

    Ok(iter)
}

fn build_plan<'query, DataToken>(
    adapter: Rc<impl Adapter<'query, DataToken = DataToken> + 'query>,
    query: &InterpretedQuery,
    plan: Vec<QueryPlanItem>,
    mut iterator: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
) -> Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>
where
    DataToken: Clone + Debug + 'query,
{
    for plan_item in plan {
        iterator = build_plan_item(adapter.clone(), query, plan_item, iterator);
    }
    iterator
}

type Iter<'query, Item> = Box<dyn Iterator<Item = Item> + 'query>;

fn build_plan_item<'query, DataToken>(
    adapter: Rc<impl Adapter<'query, DataToken = DataToken> + 'query>,
    query: &InterpretedQuery,
    plan_item: QueryPlanItem,
    iterator: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
) -> Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>
where
    DataToken: Clone + Debug + 'query,
{
    match plan_item {
        // misc steps
        QueryPlanItem::Argument(name) => {
            let right_value = query.arguments[name.as_ref()].to_owned();
            Box::new(iterator.map(move |mut ctx| {
                // TODO: implement more efficient filtering with:
                //       - no clone of runtime parameter values
                //       - omit the "tag from missing optional" check if the filter argument isn't
                //         a tag, or if it's a tag that isn't from an optional scope relative to
                //         the current scope
                //       - type awareness: we know the type of the field being filtered,
                //         and we probably know (or can infer) the type of the filtering argument(s)
                //       - precomputation to improve efficiency: build regexes once,
                //         turn "in_collection" filter arguments into sets if possible, etc.
                ctx.values.push(right_value.to_owned());
                ctx
            }))
        }
        QueryPlanItem::ImportTag(field_ref) => Box::new(iterator.map(move |mut context| {
            let value = context.imported_tags[&field_ref].clone();
            context.values.push(value);

            context
        })),
        QueryPlanItem::Record(vid) => Box::new(iterator.map(move |mut context| {
            context.record_token(vid);
            context
        })),
        QueryPlanItem::Activate(vid) => Box::new(iterator.map(move |x| x.activate_token(&vid))),
        QueryPlanItem::Unsuspend => Box::new(iterator.map(move |x| x.unsuspend())),
        QueryPlanItem::SuspendNone => Box::new(iterator.map(move |mut context| {
            if context.current_token.is_none() {
                // Mark that this token starts off with a None current_token value,
                // so the later unsuspend() call should restore it to such a state later.
                context.suspended_tokens.push(None);
            }
            context
        })),
        QueryPlanItem::Suspend => Box::new(iterator.map(move |context| context.ensure_suspended())),
        QueryPlanItem::PopIntoImport(field) => Box::new(iterator.map(move |mut ctx| {
            ctx.imported_tags.insert(
                field.clone(),
                ctx.values
                    .pop()
                    .expect("fold-specific field computed and pushed onto the stack"),
            );
            ctx
        })),
        QueryPlanItem::Filter(filter) => apply_filter(query, filter, iterator),

        // property
        QueryPlanItem::ProjectProperty {
            type_name,
            vid,
            field_name,
        } => {
            let a = adapter;
            let iter = a.project_property(iterator, type_name, field_name, query.clone(), vid);
            Box::new(iter.map(|(mut context, value)| {
                context.values.push(value);
                context
            }))
        }

        // type filter
        QueryPlanItem::Coerce {
            coerced_from,
            coerce_to,
            vid,
            kind,
        } => {
            let coercion_iter =
                adapter.can_coerce_to_type(iterator, coerced_from, coerce_to, query.clone(), vid);
            match kind {
                CoerceKind::Filter => Box::new(
                    coercion_iter.filter_map(|(ctx, can_coerce)| can_coerce.then_some(ctx)),
                ),
                CoerceKind::Suspend => Box::new(coercion_iter.map(|(ctx, can_coerce)| {
                    if can_coerce {
                        ctx
                    } else {
                        ctx.ensure_suspended()
                    }
                })),
            }
        }

        // expand
        QueryPlanItem::Neighbors {
            type_name,
            edge_name,
            parameters,
            eid,
            vid,
            kind,
        } => {
            let iter = adapter.project_neighbors(
                iterator,
                type_name,
                edge_name,
                parameters,
                query.clone(),
                vid,
                eid,
            );
            expand_neighbors(adapter, query, kind, eid, iter)
        }

        // recursion
        QueryPlanItem::RecursePostProcess => post_process_recursive_expansion(iterator),

        // fold
        QueryPlanItem::FoldCount(fold_eid) => Box::new(iterator.map(move |mut ctx| {
            let value = ctx.folded_contexts[&fold_eid].len();
            ctx.values.push(FieldValue::Uint64(value as u64));
            ctx
        })),
        QueryPlanItem::FoldOutputs {
            eid,
            vid,
            output_names,
            fold_specific_outputs,
            folded_keys,
            plan,
        } => {
            let query = query.clone();
            let iterator = iterator.map(move |mut ctx| {
                // If the @fold is inside an @optional that doesn't exist,
                // its outputs should be `null` rather than empty lists (the usual for empty folds).
                // Transformed outputs should also be `null` rather than their usual transformed defaults.
                let did_fold_root_exist = ctx.tokens[&vid].is_some();
                let default_value = did_fold_root_exist.then_some(ValueOrVec::Vec(Vec::new()));

                let fold_elements = ctx.folded_contexts.get(&eid).unwrap();

                // Add any fold-specific field outputs to the context's folded values.
                for (output_name, fold_specific_field) in &fold_specific_outputs {
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

                // Don't bother trying to resolve property values on this @fold when it's empty.
                // Skip the adapter project_property() calls and add the empty output values directly.
                if fold_elements.is_empty() {
                    // We need to make sure any outputs from any nested @fold components (recursively)
                    // are set to empty lists.
                    for key in &folded_keys {
                        folded_values.insert(key.clone(), default_value.clone());
                    }
                } else {
                    let output_iterator = build_plan(
                        adapter.clone(),
                        &query,
                        plan.clone(),
                        Box::new(fold_elements.clone().into_iter()),
                    );

                    for mut folded_context in output_iterator {
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
                };

                ctx.folded_values.extend(folded_values);
                ctx
            });
            Box::new(iterator)
        }
    }
}

fn expand_neighbors<'query, DataToken>(
    adapter: Rc<impl Adapter<'query, DataToken = DataToken> + 'query>,
    query: &InterpretedQuery,
    kind: ExpandKind,
    eid: Eid,
    iter: Iter<'query, (DataContext<DataToken>, Iter<'query, DataToken>)>,
) -> Iter<'query, DataContext<DataToken>>
where
    DataToken: Clone + Debug + 'query,
{
    match kind {
        ExpandKind::Required => Box::new(iter.flat_map(move |(context, neighbor_iterator)| {
            EdgeExpander::new(context, neighbor_iterator, false)
        })),
        ExpandKind::Optional => Box::new(iter.flat_map(move |(context, neighbor_iterator)| {
            EdgeExpander::new(context, neighbor_iterator, true)
        })),
        ExpandKind::Recursive => Box::new(iter.flat_map(move |(context, neighbor_iterator)| {
            RecursiveEdgeExpander::new(context, neighbor_iterator)
        })),
        ExpandKind::Fold {
            plan,
            tags,
            post_fold_filters,
        } => {
            let max_fold_size = get_max_fold_count_limit(query, &post_fold_filters);
            let query = query.clone();
            let folded_iterator = iter.filter_map(move |(mut context, neighbors)| {
                let imported_tags = context.imported_tags.clone();

                let neighbor_contexts = Box::new(neighbors.map(move |x| {
                    let mut ctx = DataContext::new(x);
                    ctx.imported_tags = imported_tags.clone();
                    ctx
                }));

                let computed_iterator =
                    build_plan(adapter.clone(), &query, plan.clone(), neighbor_contexts);

                let fold_elements = collect_fold_elements(computed_iterator, &max_fold_size)?;
                context
                    .folded_contexts
                    .insert_or_error(eid, fold_elements)
                    .unwrap();

                // Remove no-longer-needed imported tags.
                for imported_tag in &tags {
                    context.imported_tags.remove(imported_tag).unwrap();
                }

                Some(context)
            });
            Box::new(folded_iterator)
        }
    }
}
/// If this IRFold has a filter on the folded element count, and that filter imposes
/// a max size that can be statically determined, return that max size so it can
/// be used for further optimizations. Otherwise, return None.
fn get_max_fold_count_limit(
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

fn collect_fold_elements<'query, DataToken: Clone + Debug + 'query>(
    mut iterator: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
    max_fold_count_limit: &Option<usize>,
) -> Option<Vec<DataContext<DataToken>>> {
    if let Some(max_fold_count_limit) = max_fold_count_limit {
        // If this fold has more than `max_fold_count_limit` elements,
        // it will get filtered out by a post-fold filter.
        // Pulling elements from `iterator` causes computations and data fetches to happen,
        // and as an optimization we'd like to stop pulling elements as soon as possible.
        // If we are able to pull more than `max_fold_count_limit + 1` elements,
        // we know that this fold is going to get filtered out, so we might as well
        // stop materializing its elements early.
        let mut fold_elements = Vec::with_capacity(*max_fold_count_limit);

        let mut stopped_early = false;
        for _ in 0..*max_fold_count_limit {
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
    } else {
        // We weren't able to find any early-termination condition for materializing the fold,
        // so materialize the whole thing and return it.
        Some(iterator.collect())
    }
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
struct EdgeExpander<'query, DataToken: Clone + Debug + 'query> {
    context: DataContext<DataToken>,
    neighbor_tokens: Option<Box<dyn Iterator<Item = DataToken> + 'query>>,
    is_optional_edge: bool,
    has_neighbors: bool,
}

impl<'query, DataToken: Clone + Debug + 'query> EdgeExpander<'query, DataToken> {
    pub fn new(
        context: DataContext<DataToken>,
        neighbor_tokens: Box<dyn Iterator<Item = DataToken> + 'query>,
        is_optional_edge: bool,
    ) -> EdgeExpander<'query, DataToken> {
        EdgeExpander {
            context,
            neighbor_tokens: Some(neighbor_tokens),
            is_optional_edge,
            has_neighbors: false,
        }
    }
}

impl<'query, DataToken: Clone + Debug + 'query> Iterator for EdgeExpander<'query, DataToken> {
    type Item = DataContext<DataToken>;

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

struct RecursiveEdgeExpander<'query, DataToken: Clone + Debug + 'query> {
    context: Option<DataContext<DataToken>>,
    neighbor_base: Option<DataContext<DataToken>>,
    neighbor_tokens: Option<Box<dyn Iterator<Item = DataToken> + 'query>>,
}

impl<'query, DataToken: Clone + Debug + 'query> RecursiveEdgeExpander<'query, DataToken> {
    pub fn new(
        context: DataContext<DataToken>,
        neighbor_tokens: Box<dyn Iterator<Item = DataToken> + 'query>,
    ) -> RecursiveEdgeExpander<'query, DataToken> {
        RecursiveEdgeExpander {
            context: Some(context),
            neighbor_base: None,
            neighbor_tokens: Some(neighbor_tokens),
        }
    }
}

impl<'query, DataToken: Clone + Debug + 'query> Iterator
    for RecursiveEdgeExpander<'query, DataToken>
{
    type Item = DataContext<DataToken>;

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

fn post_process_recursive_expansion<'query, DataToken: Clone + Debug + 'query>(
    iterator: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
) -> Box<dyn Iterator<Item = DataContext<DataToken>> + 'query> {
    Box::new(
        iterator
            .flat_map(|context| unpack_piggyback(context))
            .map(|context| {
                assert!(context.piggyback.is_none());
                context.ensure_unsuspended()
            }),
    )
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
