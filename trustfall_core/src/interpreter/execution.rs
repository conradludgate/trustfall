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
    executer_components,
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
    let iter = Box::new(executer_components::collect_outputs(iter, plan.outputs));

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
            Box::new(executer_components::push_value(iterator, right_value))
        }
        QueryPlanItem::ImportTag(field_ref) => {
            Box::new(executer_components::import_tag(iterator, field_ref))
        }
        QueryPlanItem::Record(vid) => Box::new(executer_components::record(iterator, vid)),
        QueryPlanItem::Activate(vid) => Box::new(executer_components::activate(iterator, vid)),
        QueryPlanItem::Unsuspend => Box::new(executer_components::unsuspend(iterator)),
        QueryPlanItem::SuspendNone => Box::new(executer_components::suspend_none(iterator)),
        QueryPlanItem::Suspend => Box::new(executer_components::suspend(iterator)),
        QueryPlanItem::PopIntoImport(field) => {
            Box::new(executer_components::pop_into_import(iterator, field))
        }
        QueryPlanItem::Filter(filter) => apply_filter(query, filter, iterator),

        // property
        QueryPlanItem::ProjectProperty {
            type_name,
            vid,
            field_name,
        } => {
            let iter =
                adapter.project_property(iterator, type_name, field_name, query.clone(), vid);
            Box::new(executer_components::save_property_value(iter))
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
                CoerceKind::Filter => Box::new(executer_components::coerce_filter(coercion_iter)),
                CoerceKind::Suspend => Box::new(executer_components::coerce_suspend(coercion_iter)),
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
        QueryPlanItem::RecursePostProcess => Box::new(
            executer_components::post_process_recursive_expansion(iterator),
        ),

        // fold
        QueryPlanItem::FoldCount(fold_eid) => {
            Box::new(executer_components::fold_count(iterator, fold_eid))
        }
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
        ExpandKind::Required => Box::new(executer_components::expand_neighbors_required(iter)),
        ExpandKind::Optional => Box::new(executer_components::expand_neighbors_optional(iter)),
        ExpandKind::Recursive => Box::new(executer_components::expand_neighbors_recurse(iter)),
        ExpandKind::Fold {
            plan,
            tags,
            post_fold_filters,
        } => {
            let max_fold_size =
                executer_components::get_max_fold_count_limit(query, &post_fold_filters);
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

fn collect_fold_elements<'query, DataToken: Clone + Debug + 'query>(
    iterator: Box<dyn Iterator<Item = DataContext<DataToken>> + 'query>,
    max_fold_count_limit: &Option<usize>,
) -> Option<Vec<DataContext<DataToken>>> {
    if let Some(max_fold_count_limit) = max_fold_count_limit {
        executer_components::collect_fold_elements(iterator, *max_fold_count_limit)
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
