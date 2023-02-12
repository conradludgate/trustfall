use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::Arc,
};

use dbg_pls::DebugPls;

use crate::ir::{
    indexed::{EdgeKind, IndexedQuery},
    Argument, ContextField, EdgeParameters, Eid, FieldRef, FoldSpecificFieldKind, IREdge, IRFold,
    IRQueryComponent, IRVertex, LocalField, Operation, Recursive, Type, Vid,
};

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub params: Option<Arc<EdgeParameters>>,
    pub edge: Arc<str>,
    pub vid: Vid,
    pub plan: Vec<QueryPlanItem>,
    pub outputs: Vec<Arc<str>>,
    pub variables: BTreeMap<Arc<str>, Type>,
}

#[non_exhaustive]
#[derive(Debug, Clone, DebugPls)]
pub enum CoerceKind {
    /// If we cannot coerce, discard the token.
    Filter,
    /// If we cannot coerce, suspend the token.
    ///
    /// This coercion is unusual since it doesn't discard elements that can't be coerced.
    /// This is because we still want to produce those elements, and we simply want to
    /// not continue recursing deeper through them since they don't have the edge we need.
    Suspend,
}

#[non_exhaustive]
#[derive(Debug, Clone, DebugPls)]
pub enum ExpandKind {
    /// Neighbors will be flattened with their parent contexts
    Required,
    /// If there are no neighbours, expands once with null for the fields
    Optional,
    /// Neighbors will be expanded and the original context will be stored
    /// in the piggyback. Later, the `RecursionPostProcess` will unpack the piggybacked
    /// contexts
    Recursive,
    /// Neighbors will be expanded and collected, rather than flattened
    Fold {
        plan: Vec<QueryPlanItem>,
        tags: Vec<FieldRef>,
        post_fold_filters: Vec<Operation<FoldSpecificFieldKind, Argument>>,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone, DebugPls)]
pub enum QueryPlanItem {
    /// Extract a property's value from the current token
    ProjectProperty {
        type_name: Arc<str>,
        vid: Vid,
        field_name: Arc<str>,
    },
    /// Try to convert the current value to the new type
    Coerce {
        coerced_from: Arc<str>,
        coerce_to: Arc<str>,
        vid: Vid,
        kind: CoerceKind,
    },
    /// Follow the edge from this vertex to find new neighbors
    Neighbors {
        type_name: Arc<str>,
        edge_name: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
        eid: Eid,
        vid: Vid,
        kind: ExpandKind,
    },

    /// Take a value from the imported tags and push it onto the value stack
    ImportTag(FieldRef),
    /// Take the last value in the value stack and push it onto the
    /// imported tags
    PopIntoImport(FieldRef),

    /// Take the given argument and push it onto the value stack
    Argument(Arc<str>),
    /// Save the number of items in the fold to the value stack
    FoldCount(Eid),

    /// Perform a filtering using the expression, with the last
    /// value in the stack as the left hand side
    Filter(Operation<(), Argument>),

    /// Record the current token at the given vertex
    Record(Vid),
    /// Change the current token to the previously recorded token at
    /// the given vertex
    Activate(Vid),
    /// Change the current token to the previously suspended token
    Unsuspend,
    /// If there is no current token, insert an empty token into the suspended
    /// stack
    SuspendNone,
    /// Suspend the current context
    Suspend,

    /// Collect the outputs from fold
    FoldOutputs {
        eid: Eid,
        vid: Vid,
        output_names: Vec<Arc<str>>,
        fold_specific_outputs: BTreeMap<Arc<str>, FoldSpecificFieldKind>,
        folded_keys: BTreeSet<(Eid, Arc<str>)>,
        plan: Vec<QueryPlanItem>,
    },

    /// Flatten the output from recursion
    RecursePostProcess,
}

pub fn query_plan(indexed_query: Arc<IndexedQuery>) -> QueryPlan {
    let ir_query = &indexed_query.ir_query;

    let root_edge = ir_query.root_name.clone();
    let root_edge_params = ir_query.root_parameters.clone();
    let component = &ir_query.root_component;
    let root_vid = component.root;

    let mut plan = compute_component(indexed_query.clone(), component);
    let outputs = construct_outputs(&ir_query.root_component, &mut plan);

    QueryPlan {
        plan,
        params: root_edge_params,
        edge: root_edge,
        vid: root_vid,
        outputs,
        variables: indexed_query.ir_query.variables.clone(),
    }
}

fn coerce_if_needed(vertex: &IRVertex, output: &mut Vec<QueryPlanItem>) {
    if let Some(coerced_from) = vertex.coerced_from_type.as_ref() {
        output.push(QueryPlanItem::Coerce {
            coerced_from: coerced_from.clone(),
            coerce_to: vertex.type_name.clone(),
            vid: vertex.vid,
            kind: CoerceKind::Filter,
        });
    }
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

    let iter = OrderedIter {
        edge: component.edges.values(),
        fold: component.folds.values(),
        previous: None,
    };

    for next in iter {
        match next {
            EdgeKind::Fold(fold) => {
                compute_fold(
                    query.clone(),
                    &component.vertices[&fold.from_vid],
                    component,
                    fold,
                    &mut output,
                );
            }
            EdgeKind::Regular(edge) => {
                expand_edge(component, edge.from_vid, edge.to_vid, &edge, &mut output);
            }
        }
    }

    output
}

fn construct_outputs(
    component: &IRQueryComponent,
    output: &mut Vec<QueryPlanItem>,
) -> Vec<Arc<str>> {
    let mut output_names: Vec<Arc<str>> = component.outputs.keys().cloned().collect();
    output_names.sort_unstable(); // to ensure deterministic project_property() ordering

    let mut last_vertex = None;
    for output_name in &output_names {
        let context_field = &component.outputs[output_name.as_ref()];
        let vertex_id = context_field.vertex_id;
        if last_vertex != Some(vertex_id) {
            output.push(QueryPlanItem::Activate(vertex_id));
            last_vertex = Some(vertex_id);
        }
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
    let mut last_vertex = None;
    for imported_field in fold.imported_tags.iter() {
        match &imported_field {
            FieldRef::ContextField(field) => {
                let vertex_id = field.vertex_id;
                if last_vertex != Some(vertex_id) {
                    output.push(QueryPlanItem::Activate(vertex_id));
                    last_vertex = Some(vertex_id);
                }

                let field_vertex = &parent_component.vertices[&field.vertex_id];
                let type_name = field_vertex.type_name.clone();
                output.push(QueryPlanItem::ProjectProperty {
                    type_name,
                    vid: field.vertex_id,
                    field_name: field.field_name.clone(),
                });
            }
            FieldRef::FoldSpecificField(fold_specific_field) => {
                compute_fold_specific_field(
                    fold_specific_field.fold_eid,
                    &fold_specific_field.kind,
                    output,
                );
            }
        }
        output.push(QueryPlanItem::PopIntoImport(imported_field.clone()));
    }

    // Get the initial vertices inside the folded scope.
    let expanding_from_vid = expanding_from.vid;
    output.push(QueryPlanItem::Activate(expanding_from_vid));

    let type_name = expanding_from.type_name.clone();

    output.push(QueryPlanItem::Neighbors {
        type_name,
        edge_name: fold.edge_name.clone(),
        parameters: fold.parameters.clone(),
        vid: expanding_from.vid,
        eid: fold.eid,
        kind: ExpandKind::Fold {
            plan: compute_component(query, &fold.component),
            tags: fold.imported_tags.clone(),
            post_fold_filters: fold.post_filters.clone(),
        },
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
    let output_names = construct_outputs(&fold.component, &mut fold_outputs);

    let mut folded_keys: BTreeSet<(Eid, Arc<str>)> = Default::default();
    // We need to make sure any outputs from any nested @fold components (recursively)
    // are set to empty lists.
    let mut queue: Vec<_> = fold.component.folds.values().collect();
    while let Some(inner_fold) = queue.pop() {
        for output in inner_fold.fold_specific_outputs.keys() {
            folded_keys.insert((inner_fold.eid, output.clone()));
        }
        for output in inner_fold.component.outputs.keys() {
            folded_keys.insert((inner_fold.eid, output.clone()));
        }
        queue.extend(inner_fold.component.folds.values());
    }

    output.push(QueryPlanItem::FoldOutputs {
        eid: fold.eid,
        vid: expanding_from_vid,
        output_names,
        fold_specific_outputs: fold.fold_specific_outputs.clone(),
        folded_keys,
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
        output.push(QueryPlanItem::Suspend);
        output.push(QueryPlanItem::Activate(vertex_id));

        let type_name = vertex.type_name.clone();
        output.push(QueryPlanItem::ProjectProperty {
            type_name,
            vid: vertex_id,
            field_name: context_field.field_name.clone(),
        });
        output.push(QueryPlanItem::Unsuspend);
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

    output.push(QueryPlanItem::Neighbors {
        type_name: expanding_from.type_name.clone(),
        edge_name: edge_name.clone(),
        parameters: edge_parameters.clone(),
        eid: edge_id,
        vid: expanding_from.vid,
        kind: if is_optional {
            ExpandKind::Optional
        } else {
            ExpandKind::Required
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
            output.push(QueryPlanItem::Coerce {
                coerced_from: edge_endpoint_type.clone(),
                coerce_to: coerce_to.clone(),
                vid: expanding_from.vid,
                kind: CoerceKind::Suspend,
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
    output.push(QueryPlanItem::Neighbors {
        type_name: expanding_from_type,
        edge_name: edge_name.clone(),
        parameters: edge_parameters.clone(),
        eid: edge_id,
        vid: expanding_from.vid,
        kind: ExpandKind::Recursive,
    });
}

/// Returns folds/edges ordered by their [`Eid`]
struct OrderedIter<'a> {
    edge: std::collections::btree_map::Values<'a, Eid, Arc<IREdge>>,
    fold: std::collections::btree_map::Values<'a, Eid, Arc<IRFold>>,
    previous: Option<EdgeKind>,
}

impl Iterator for OrderedIter<'_> {
    type Item = EdgeKind;
    fn next(&mut self) -> Option<Self::Item> {
        match self.previous.take() {
            // if the last saved value was an edge, try compare it to a fold
            Some(EdgeKind::Regular(edge)) => match self.fold.next() {
                // edge comes first
                Some(fold) if edge.eid < fold.eid => {
                    self.previous = Some(EdgeKind::Fold(fold.clone()));
                    Some(EdgeKind::Regular(edge))
                }
                // fold comes first
                Some(fold) => {
                    self.previous = Some(EdgeKind::Regular(edge));
                    Some(EdgeKind::Fold(fold.clone()))
                }
                None => Some(EdgeKind::Regular(edge)),
            },
            // if the last saved value was a fold, try compare it to an edge
            Some(EdgeKind::Fold(fold)) => match self.edge.next() {
                // edge comes first
                Some(edge) if edge.eid < fold.eid => {
                    self.previous = Some(EdgeKind::Fold(fold));
                    Some(EdgeKind::Regular(edge.clone()))
                }
                // fold comes first
                Some(edge) => {
                    self.previous = Some(EdgeKind::Regular(edge.clone()));
                    Some(EdgeKind::Fold(fold))
                }
                None => Some(EdgeKind::Fold(fold)),
            },
            // if there was no saved value, get both and try
            None => match (self.edge.next(), self.fold.next()) {
                (None, None) => None,
                (None, Some(fold)) => Some(EdgeKind::Fold(fold.clone())),
                (Some(edge), None) => Some(EdgeKind::Regular(edge.clone())),
                // edge comes first
                (Some(edge), Some(fold)) if edge.eid < fold.eid => {
                    self.previous = Some(EdgeKind::Fold(fold.clone()));
                    Some(EdgeKind::Regular(edge.clone()))
                }
                // fold comes first
                (Some(edge), Some(fold)) => {
                    self.previous = Some(EdgeKind::Regular(edge.clone()));
                    Some(EdgeKind::Fold(fold.clone()))
                }
            },
        }
    }
}
