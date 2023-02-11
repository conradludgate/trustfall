use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::Arc,
};

use crate::ir::{
    indexed::IndexedQuery, Argument, ContextField, EdgeParameters, Eid, FieldRef,
    FoldSpecificFieldKind, IREdge, IRFold, IRQueryComponent, IRVertex, LocalField, Operation,
    Recursive, Vid,
};

#[derive(Debug)]
pub struct QueryPlan {
    edge: Arc<str>,
    vid: Vid,
    plan: Vec<QueryPlanItem>,
}

#[derive(Debug)]
pub struct ProjectNeighbors {
    type_name: Arc<str>,
    edge_name: Arc<str>,
    parameters: Option<Arc<EdgeParameters>>,
    eid: Eid,
    vid: Vid,
}

#[non_exhaustive]
#[derive(Debug)]
pub enum QueryPlanItem {
    Fold {
        neighbors: ProjectNeighbors,
        plan: Vec<QueryPlanItem>,
        tags: Vec<FieldRef>,
    },
    Recurse {
        neighbors: ProjectNeighbors,
    },
    Expand {
        optional: bool,
        neighbors: ProjectNeighbors,
    },
    Coerce {
        coerced_from: Arc<str>,
        coerce_to: Arc<str>,
        vid: Vid,
    },
    // This coercion is unusual since it doesn't discard elements that can't be coerced.
    // This is because we still want to produce those elements, and we simply want to
    // not continue recursing deeper through them since they don't have the edge we need.
    CoerceOrSuspend {
        coerced_from: Arc<str>,
        coerce_to: Arc<str>,
        vid: Vid,
    },
    ProjectProperty {
        type_name: Arc<str>,
        vid: Vid,
        field_name: Arc<str>,
    },
    ProjectPropertyImported {
        type_name: Arc<str>,
        vid: Vid,
        field_name: Arc<str>,
        imported_field: FieldRef,
    },
    Argument(Arc<str>),
    ImportTag(FieldRef),
    MoveTo(Vid),
    PopSuspended,
    FoldCount(Eid),
    Record(Vid),
    Activate(Vid),
    SuspendNone,
    CollectOutputs(Vec<Arc<str>>),
    PopIntoImport(FieldRef),
    Filter(Operation<(), Argument>),
    FoldOutputs {
        eid: Eid,
        vid: Vid,
        output_names: Vec<Arc<str>>,
        outputs: BTreeMap<Arc<str>, ContextField>,
        fold_specific_outputs: BTreeMap<Arc<str>, FoldSpecificFieldKind>,
        folds: BTreeMap<Eid, Arc<IRFold>>,
        plan: Vec<QueryPlanItem>,
    },
    RecursePostProcess,
}

pub fn query_plan(indexed_query: Arc<IndexedQuery>) -> QueryPlan {
    let ir_query = &indexed_query.ir_query;

    let root_edge = ir_query.root_name.clone();
    let component = &ir_query.root_component;
    let root_vid = component.root;

    let mut plan = compute_component(indexed_query.clone(), component);
    construct_outputs(indexed_query, &mut plan);

    QueryPlan {
        plan,
        edge: root_edge,
        vid: root_vid,
    }
}

fn coerce_if_needed(vertex: &IRVertex, output: &mut Vec<QueryPlanItem>) {
    if let Some(coerced_from) = vertex.coerced_from_type.as_ref() {
        perform_coercion(
            vertex,
            coerced_from.clone(),
            vertex.type_name.clone(),
            output,
        );
    }
}

fn perform_coercion(
    vertex: &IRVertex,
    coerced_from: Arc<str>,
    coerce_to: Arc<str>,
    output: &mut Vec<QueryPlanItem>,
) {
    output.push(QueryPlanItem::Coerce {
        coerced_from,
        coerce_to,
        vid: vertex.vid,
    });
}

fn compute_component(query: Arc<IndexedQuery>, component: &IRQueryComponent) -> Vec<QueryPlanItem> {
    let mut output =
        Vec::with_capacity((query.vids.len() + query.eids.len() + query.outputs.len()) * 3);

    let component_root_vid = component.root;
    let root_vertex = &component.vertices[&component_root_vid];

    coerce_if_needed(root_vertex, &mut output);

    for filter_expr in &root_vertex.filters {
        apply_local_field_filter(component, component.root, filter_expr, &mut output);
    }

    output.push(QueryPlanItem::Record(component_root_vid));

    let mut visited_vids: BTreeSet<Vid> = btreeset! {component_root_vid};

    let mut edge_iter = component.edges.values();
    let mut fold_iter = component.folds.values();
    let mut next_edge = edge_iter.next();
    let mut next_fold = fold_iter.next();
    loop {
        let (process_next_fold, process_next_edge) = match (next_fold, next_edge) {
            (None, None) => break,
            (None, Some(_)) | (Some(_), None) => (next_fold, next_edge),
            (Some(fold), Some(edge)) => match fold.eid.cmp(&edge.eid) {
                std::cmp::Ordering::Greater => (None, Some(edge)),
                std::cmp::Ordering::Less => (Some(fold), None),
                std::cmp::Ordering::Equal => unreachable!(),
            },
        };

        assert!(process_next_fold.is_some() ^ process_next_edge.is_some());

        if let Some(fold) = process_next_fold {
            let from_vid_unvisited = visited_vids.insert(fold.from_vid);
            let to_vid_unvisited = visited_vids.insert(fold.to_vid);
            assert!(!from_vid_unvisited);
            assert!(to_vid_unvisited);

            compute_fold(
                query.clone(),
                &component.vertices[&fold.from_vid],
                component,
                fold.clone(),
                &mut output,
            );

            next_fold = fold_iter.next();
        } else if let Some(edge) = process_next_edge {
            let from_vid_unvisited = visited_vids.insert(edge.from_vid);
            let to_vid_unvisited = visited_vids.insert(edge.to_vid);
            assert!(!from_vid_unvisited);
            assert!(to_vid_unvisited);

            expand_edge(component, edge.from_vid, edge.to_vid, edge, &mut output);

            next_edge = edge_iter.next();
        }
    }

    output
}

fn construct_outputs(query: Arc<IndexedQuery>, output: &mut Vec<QueryPlanItem>) {
    let output_names = outputs(&query.ir_query.root_component, output);
    output.push(QueryPlanItem::CollectOutputs(output_names));
}

fn outputs(component: &IRQueryComponent, output: &mut Vec<QueryPlanItem>) -> Vec<Arc<str>> {
    let mut output_names: Vec<Arc<str>> = component.outputs.keys().cloned().collect();
    output_names.sort_unstable(); // to ensure deterministic project_property() ordering

    for output_name in output_names.iter() {
        let context_field = &component.outputs[output_name.as_ref()];
        let vertex_id = context_field.vertex_id;
        output.push(QueryPlanItem::MoveTo(vertex_id));
        output.push(QueryPlanItem::ProjectProperty {
            type_name: component.vertices[&vertex_id].type_name.clone(),
            vid: vertex_id,
            field_name: context_field.field_name.clone(),
        })
    }

    output_names
}

fn compute_fold(
    query: Arc<IndexedQuery>,
    expanding_from: &IRVertex,
    parent_component: &IRQueryComponent,
    fold: Arc<IRFold>,
    output: &mut Vec<QueryPlanItem>,
) {
    // Get any imported tag values needed inside the fold component or one of its subcomponents.
    for imported_field in fold.imported_tags.iter() {
        match &imported_field {
            FieldRef::ContextField(field) => {
                let vertex_id = field.vertex_id;
                output.push(QueryPlanItem::Activate(vertex_id));

                let field_vertex = &parent_component.vertices[&field.vertex_id];
                let type_name = field_vertex.type_name.clone();
                output.push(QueryPlanItem::ProjectPropertyImported {
                    type_name,
                    vid: field.vertex_id,
                    field_name: field.field_name.clone(),
                    imported_field: imported_field.clone(),
                });
            }
            FieldRef::FoldSpecificField(fold_specific_field) => {
                let cloned_field = imported_field.clone();
                compute_fold_specific_field(
                    fold_specific_field.fold_eid,
                    &fold_specific_field.kind,
                    output,
                );

                output.push(QueryPlanItem::PopIntoImport(cloned_field));
            }
        }
    }

    // Get the initial vertices inside the folded scope.
    let expanding_from_vid = expanding_from.vid;
    output.push(QueryPlanItem::Activate(expanding_from_vid));

    let type_name = expanding_from.type_name.clone();
    let neighbors = ProjectNeighbors {
        type_name,
        edge_name: fold.edge_name.clone(),
        parameters: fold.parameters.clone(),
        vid: expanding_from.vid,
        eid: fold.eid,
    };

    output.push(QueryPlanItem::Fold {
        neighbors,
        plan: compute_component(query, &fold.component),
        tags: fold.imported_tags.clone(),
    });

    // Apply post-fold filters.
    for post_fold_filter in fold.post_filters.iter() {
        apply_fold_specific_filter(
            parent_component,
            fold.as_ref(),
            expanding_from.vid,
            post_fold_filter,
            output,
        );
    }

    let mut fold_outputs = vec![];
    let output_names = outputs(&fold.component, &mut fold_outputs);
    output.push(QueryPlanItem::FoldOutputs {
        eid: fold.eid,
        vid: expanding_from_vid,
        output_names,
        fold_specific_outputs: fold.fold_specific_outputs.clone(),
        outputs: fold.component.outputs.clone(),
        folds: fold.component.folds.clone(),
        plan: fold_outputs,
    });
}

fn apply_local_field_filter(
    component: &IRQueryComponent,
    current_vid: Vid,
    filter: &Operation<LocalField, Argument>,
    output: &mut Vec<QueryPlanItem>,
) {
    let local_field = filter.left();
    compute_local_field(component, current_vid, local_field, output);

    apply_filter(component, current_vid, filter, output);
}

fn apply_fold_specific_filter(
    component: &IRQueryComponent,
    fold: &IRFold,
    current_vid: Vid,
    filter: &Operation<FoldSpecificFieldKind, Argument>,
    output: &mut Vec<QueryPlanItem>,
) {
    let fold_specific_field = filter.left();
    compute_fold_specific_field(fold.eid, fold_specific_field, output);
    apply_filter(component, current_vid, filter, output)
}

fn apply_filter<LeftT: Debug + Clone + PartialEq + Eq>(
    component: &IRQueryComponent,
    current_vid: Vid,
    filter: &Operation<LeftT, Argument>,
    output: &mut Vec<QueryPlanItem>,
) {
    match filter.right() {
        Some(Argument::Tag(FieldRef::ContextField(context_field))) => {
            if context_field.vertex_id == current_vid {
                // This tag is from the vertex we're currently filtering. That means the field
                // whose value we want to get is actually local, so there's no need to compute it
                // using the more expensive approach we use for non-local fields.
                let local_equivalent_field = LocalField {
                    field_name: context_field.field_name.clone(),
                    field_type: context_field.field_type.clone(),
                };
                compute_local_field(component, current_vid, &local_equivalent_field, output)
            } else {
                compute_context_field(component, context_field, output);
            }
        }
        Some(Argument::Tag(field_ref @ FieldRef::FoldSpecificField(fold_field))) => {
            if component.folds.contains_key(&fold_field.fold_eid) {
                // This value comes from one of this component's folds:
                // the @tag is a sibling to the current computation and needs to be materialized.
                compute_fold_specific_field(fold_field.fold_eid, &fold_field.kind, output)
            } else {
                // This value represents an imported tag value from an outer component.
                // Grab its value from the context itself.
                output.push(QueryPlanItem::ImportTag(field_ref.clone()));
            }
        }
        Some(Argument::Variable(var)) => {
            output.push(QueryPlanItem::Argument(var.variable_name.clone()));
        }
        None => {}
    };

    output.push(QueryPlanItem::Filter(filter.clone().right_only()));
}

fn compute_context_field(
    component: &IRQueryComponent,
    context_field: &ContextField,
    output: &mut Vec<QueryPlanItem>,
) {
    let vertex_id = context_field.vertex_id;

    if let Some(vertex) = component.vertices.get(&vertex_id) {
        output.push(QueryPlanItem::MoveTo(vertex_id));

        let type_name = vertex.type_name.clone();
        output.push(QueryPlanItem::ProjectProperty {
            type_name,
            vid: vertex_id,
            field_name: context_field.field_name.clone(),
        });

        output.push(QueryPlanItem::PopSuspended);
    } else {
        // This context field represents an imported tag value from an outer component.
        // Grab its value from the context itself.
        let field_ref = FieldRef::ContextField(context_field.clone());
        output.push(QueryPlanItem::ImportTag(field_ref));
    }
}

fn compute_fold_specific_field(
    fold_eid: Eid,
    fold_specific_field: &FoldSpecificFieldKind,
    output: &mut Vec<QueryPlanItem>,
) {
    match fold_specific_field {
        FoldSpecificFieldKind::Count => output.push(QueryPlanItem::FoldCount(fold_eid)),
    }
}

fn compute_local_field(
    component: &IRQueryComponent,
    current_vid: Vid,
    local_field: &LocalField,
    output: &mut Vec<QueryPlanItem>,
) {
    let type_name = component.vertices[&current_vid].type_name.clone();
    output.push(QueryPlanItem::ProjectProperty {
        type_name,
        vid: current_vid,
        field_name: local_field.field_name.clone(),
    });
}

fn expand_edge(
    component: &IRQueryComponent,
    expanding_from_vid: Vid,
    expanding_to_vid: Vid,
    edge: &IREdge,
    output: &mut Vec<QueryPlanItem>,
) {
    if let Some(recursive) = &edge.recursive {
        expand_recursive_edge(
            &component.vertices[&expanding_from_vid],
            &component.vertices[&expanding_to_vid],
            edge.eid,
            &edge.edge_name,
            &edge.parameters,
            recursive,
            output,
        )
    } else {
        expand_non_recursive_edge(
            &component.vertices[&expanding_from_vid],
            edge.eid,
            &edge.edge_name,
            &edge.parameters,
            edge.optional,
            output,
        )
    };

    perform_entry_into_new_vertex(component, &component.vertices[&expanding_to_vid], output)
}

fn expand_non_recursive_edge(
    expanding_from: &IRVertex,
    edge_id: Eid,
    edge_name: &Arc<str>,
    edge_parameters: &Option<Arc<EdgeParameters>>,
    is_optional: bool,
    output: &mut Vec<QueryPlanItem>,
) {
    output.push(QueryPlanItem::Activate(expanding_from.vid));

    output.push(QueryPlanItem::Expand {
        optional: is_optional,
        neighbors: ProjectNeighbors {
            type_name: expanding_from.type_name.clone(),
            edge_name: edge_name.clone(),
            parameters: edge_parameters.clone(),
            eid: edge_id,
            vid: expanding_from.vid,
        },
    });
}

/// Apply all the operations needed at entry into a new vertex:
/// - coerce the type, if needed
/// - apply all local filters
/// - record the token at this Vid in the context
fn perform_entry_into_new_vertex(
    component: &IRQueryComponent,
    vertex: &IRVertex,
    output: &mut Vec<QueryPlanItem>,
) {
    let vertex_id = vertex.vid;
    coerce_if_needed(vertex, output);
    for filter_expr in vertex.filters.iter() {
        apply_local_field_filter(component, vertex_id, filter_expr, output);
    }
    output.push(QueryPlanItem::Record(vertex_id));
}

fn expand_recursive_edge(
    expanding_from: &IRVertex,
    expanding_to: &IRVertex,
    edge_id: Eid,
    edge_name: &Arc<str>,
    edge_parameters: &Option<Arc<EdgeParameters>>,
    recursive: &Recursive,
    output: &mut Vec<QueryPlanItem>,
) {
    output.push(QueryPlanItem::SuspendNone);
    output.push(QueryPlanItem::Activate(expanding_from.vid));

    let max_depth = usize::from(recursive.depth);
    perform_one_recursive_edge_expansion(
        expanding_from.type_name.clone(),
        expanding_from,
        edge_id,
        edge_name,
        edge_parameters,
        output,
    );

    let edge_endpoint_type = expanding_to
        .coerced_from_type
        .as_ref()
        .unwrap_or(&expanding_to.type_name);
    let recursing_from = recursive.coerce_to.as_ref().unwrap_or(edge_endpoint_type);

    for _ in 2..=max_depth {
        if let Some(coerce_to) = recursive.coerce_to.as_ref() {
            output.push(QueryPlanItem::CoerceOrSuspend {
                coerced_from: edge_endpoint_type.clone(),
                coerce_to: coerce_to.clone(),
                vid: expanding_from.vid,
            });
        }

        perform_one_recursive_edge_expansion(
            recursing_from.clone(),
            expanding_from,
            edge_id,
            edge_name,
            edge_parameters,
            output,
        );
    }

    output.push(QueryPlanItem::RecursePostProcess);
}

fn perform_one_recursive_edge_expansion(
    expanding_from_type: Arc<str>,
    expanding_from: &IRVertex,
    edge_id: Eid,
    edge_name: &Arc<str>,
    edge_parameters: &Option<Arc<EdgeParameters>>,
    output: &mut Vec<QueryPlanItem>,
) {
    output.push(QueryPlanItem::Recurse {
        neighbors: ProjectNeighbors {
            type_name: expanding_from_type,
            edge_name: edge_name.clone(),
            parameters: edge_parameters.clone(),
            eid: edge_id,
            vid: expanding_from.vid,
        },
    });
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
            Arc::from(indexed_query),
            Arc::from(arguments),
        );

        let check_parsed: Result<_, QueryArgumentsError> = Err(ron::from_str(&check_data).unwrap());

        assert_eq!(check_parsed, constructed_test_item);
    }
}
