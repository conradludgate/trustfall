use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::Arc,
};

use actix::{Actor, Context, Handler, Message, Recipient};
use tracing::instrument;

use crate::ir::{
    indexed::IndexedQuery, EdgeParameters, Eid, FieldValue, IREdge, IRQueryComponent, IRVertex, Vid,
};

use super::{ActixAdapter, DataContext, Edge, InterpretedQuery, Node, Property, Vertex};

struct InitActor<Token: Send + Debug + Clone> {
    recipient: Recipient<Vertex<Token>>,
}

impl<DataToken: Clone + Debug + Send + 'static + Unpin> Actor for InitActor<DataToken> {
    type Context = Context<Self>;
}

impl<DataToken: Clone + Debug + Send + 'static + Unpin> Handler<Node<DataToken>>
    for InitActor<DataToken>
{
    type Result = ();

    fn handle(&mut self, msg: Node<DataToken>, _ctx: &mut Self::Context) -> Self::Result {
        self.recipient.do_send(Vertex(DataContext::new(Some(msg.0))));
    }
}

#[allow(clippy::type_complexity)]
#[instrument(skip_all)]
pub fn interpret_ir<DataToken>(
    adapter: Arc<impl ActixAdapter<DataToken = DataToken>>,
    indexed_query: Arc<IndexedQuery>,
    arguments: Arc<BTreeMap<Arc<str>, FieldValue>>,
    recipient: Recipient<Output>,
) where
    DataToken: Clone + Debug + Send + 'static + Unpin,
{
    let query = InterpretedQuery::from_query_and_arguments(indexed_query, arguments).unwrap();

    let recipient = construct_outputs(adapter.as_ref(), &query, recipient);

    let ir_query = &query.indexed_query.ir_query;
    let component = &ir_query.root_component;
    let recipient = compute_component(adapter.clone(), &query, component, recipient);

    let recipient = InitActor { recipient }.start().recipient();

    let root_edge = ir_query.root_name.clone();
    let root_edge_parameters = ir_query.root_parameters.clone();

    adapter
        .edge_actor("@".into(), root_edge, root_edge_parameters)
        .do_send(Edge(DataContext::new(None), recipient));
}

#[instrument(skip_all)]
fn coerce_if_needed<DataToken>(
    adapter: &impl ActixAdapter<DataToken = DataToken>,
    _query: &InterpretedQuery,
    vertex: &IRVertex,
    recipient: Recipient<Vertex<DataToken>>,
) -> Recipient<Vertex<DataToken>>
where
    DataToken: Clone + Debug + Send + 'static + Unpin,
{
    match vertex.coerced_from_type.as_ref() {
        None => recipient,
        Some(coerced_from) => {
            adapter.coerce_actor(coerced_from.clone(), vertex.type_name.clone(), recipient)
        }
    }
}

#[instrument(skip_all)]
fn compute_component<DataToken>(
    adapter: Arc<impl ActixAdapter<DataToken = DataToken>>,
    query: &InterpretedQuery,
    component: &IRQueryComponent,
    mut recipient: Recipient<Vertex<DataToken>>,
) -> Recipient<Vertex<DataToken>>
where
    DataToken: Clone + Debug + Send + 'static + Unpin,
{
    let component_root_vid = component.root;
    let root_vertex = &component.vertices[&component_root_vid];

    let mut visited_vids: BTreeSet<Vid> = btreeset! {component_root_vid};

    for edge in component.edges.values() {
        let from_vid_unvisited = visited_vids.insert(edge.from_vid);
        let to_vid_unvisited = visited_vids.insert(edge.to_vid);
        assert!(!from_vid_unvisited);
        assert!(to_vid_unvisited);

        recipient = expand_edge(
            adapter.clone(),
            query,
            component,
            edge.from_vid,
            edge.to_vid,
            edge,
            recipient,
        );
    }

    coerce_if_needed(adapter.as_ref(), query, root_vertex, recipient)
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Output(pub BTreeMap<Arc<str>, FieldValue>);

struct OutputActor {
    recipient: Recipient<Output>,
    output_names: Vec<Arc<str>>,
}

impl Actor for OutputActor {
    type Context = Context<Self>;
}

impl<DataToken: Clone + Debug + Send + 'static + Unpin> Handler<Vertex<DataToken>> for OutputActor {
    type Result = ();

    fn handle(&mut self, mut msg: Vertex<DataToken>, _ctx: &mut Self::Context) -> Self::Result {
        assert!(msg.0.values.len() == self.output_names.len());

        let mut output: BTreeMap<Arc<str>, FieldValue> = self
            .output_names
            .iter()
            .cloned()
            .zip(msg.0.values.drain(..))
            .collect();

        for ((_, output_name), output_value) in msg.0.folded_values {
            let existing = output.insert(output_name, output_value.into());
            assert!(existing.is_none());
        }

        self.recipient.do_send(Output(output));
    }
}

struct OutputActor3<Token: Clone + Debug + Send> {
    recipient: Recipient<Vertex<Token>>,
    vertex_id: Vid,
}

impl<DataToken: Clone + Debug + Send + 'static + Unpin> Actor for OutputActor3<DataToken> {
    type Context = Context<Self>;
}

impl<DataToken: Clone + Debug + Send + 'static + Unpin> Handler<Vertex<DataToken>>
    for OutputActor3<DataToken>
{
    type Result = ();

    fn handle(&mut self, msg: Vertex<DataToken>, _ctx: &mut Self::Context) -> Self::Result {
        let Vertex(mut context) = msg;
        let new_token = context.tokens[&self.vertex_id].clone();
        context = context.move_to_token(new_token);
        self.recipient.do_send(Vertex(context));
    }
}

struct OutputActor2<Token: Clone + Debug + Send> {
    recipient: Recipient<Vertex<Token>>,
    vertex_id: Option<Vid>,
}

impl<DataToken: Clone + Debug + Send + 'static + Unpin> Actor for OutputActor2<DataToken> {
    type Context = Context<Self>;
}

impl<DataToken: Clone + Debug + Send + 'static + Unpin> Handler<Property<DataToken>>
    for OutputActor2<DataToken>
{
    type Result = ();

    fn handle(&mut self, msg: Property<DataToken>, _ctx: &mut Self::Context) -> Self::Result {
        let Property(mut context, value) = msg;
        context.values.push(value);
        if let Some(vertex_id) = &self.vertex_id {
            let new_token = context.tokens[vertex_id].clone();
            context = context.move_to_token(new_token)
        };
        self.recipient.do_send(Vertex(context));
    }
}

#[instrument(skip_all)]
fn construct_outputs<DataToken: Clone + Debug + Send + 'static + Unpin>(
    adapter: &impl ActixAdapter<DataToken = DataToken>,
    query: &InterpretedQuery,
    recipient: Recipient<Output>,
) -> Recipient<Vertex<DataToken>> {
    let ir_query = &query.indexed_query.ir_query;
    let mut output_names: Vec<Arc<str>> = ir_query.root_component.outputs.keys().cloned().collect();
    output_names.sort_unstable(); // to ensure deterministic project_property() ordering

    let recipient = OutputActor {
        recipient,
        output_names: output_names.clone(),
    }
    .start()
    .recipient();

    let mut recipient = OutputActor2 {
        recipient,
        vertex_id: None,
    }
    .start()
    .recipient();

    // we should have at least 1, right?
    let mut iter = output_names.iter();
    let mut output_name = iter.next().unwrap();

    let mut context_field = &ir_query.root_component.outputs[output_name];
    let mut vertex_id = context_field.vertex_id;

    loop {
        let current_type_name = &ir_query.root_component.vertices[&vertex_id].type_name;

        let field_data_recipient = adapter.property_actor(
            current_type_name.clone(),
            context_field.field_name.clone(),
            recipient,
        );

        match iter.next() {
            Some(x) => {
                recipient = OutputActor2 {
                    recipient: field_data_recipient,
                    vertex_id: Some(vertex_id),
                }
                .start()
                .recipient();
                output_name = x;
                context_field = &ir_query.root_component.outputs[output_name];
                vertex_id = context_field.vertex_id;
            }
            None => {
                return OutputActor3 {
                    recipient: field_data_recipient,
                    vertex_id,
                }
                .start()
                .recipient()
            }
        }
    }
}

#[instrument(skip_all)]
fn expand_edge<DataToken: Clone + Debug + Send + 'static + Unpin>(
    adapter: Arc<impl ActixAdapter<DataToken = DataToken>>,
    query: &InterpretedQuery,
    component: &IRQueryComponent,
    expanding_from_vid: Vid,
    expanding_to_vid: Vid,
    edge: &IREdge,
    recipient: Recipient<Vertex<DataToken>>,
) -> Recipient<Vertex<DataToken>> {
    let recipient = perform_entry_into_new_vertex(
        adapter.clone(),
        query,
        component,
        &component.vertices[&expanding_to_vid],
        recipient,
    );

    expand_non_recursive_edge(
        adapter,
        query,
        component,
        &component.vertices[&expanding_from_vid],
        &component.vertices[&expanding_to_vid],
        edge.eid,
        &edge.edge_name,
        &edge.parameters,
        edge.optional,
        recipient,
    )
}

struct EdgeExpanderActor<Token: Debug + Clone + Send + 'static> {
    recipient1: Recipient<Edge<Token>>,
    recipient2: Recipient<Vertex<Token>>,
    is_optional_edge: bool,
}

impl<Token: Debug + Clone + Send + 'static> Actor for EdgeExpanderActor<Token> {
    type Context = Context<Self>;
}

impl<Token: Debug + Clone + Send + 'static + Unpin> Handler<Vertex<Token>>
    for EdgeExpanderActor<Token>
{
    type Result = ();

    fn handle(&mut self, msg: Vertex<Token>, _ctx: &mut Self::Context) -> Self::Result {
        let two = EdgeExpanderActor2 {
            context: msg.0.clone(),
            is_optional_edge: self.is_optional_edge,
            has_neighbors: false,
            recipient: self.recipient2.clone(),
        }
        .start()
        .recipient();

        self.recipient1.do_send(Edge(msg.0, two));

        todo!()
    }
}

struct EdgeExpanderActor2<Token: Debug + Clone + Send + 'static> {
    context: DataContext<Token>,
    is_optional_edge: bool,
    has_neighbors: bool,

    recipient: Recipient<Vertex<Token>>,
}

impl<Token: Debug + Clone + Send + 'static + Unpin> Actor for EdgeExpanderActor2<Token> {
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // If the current token is None, that means that a prior edge was optional and missing.
        // In that case, we couldn't possibly have found any neighbors here, but the optional-ness
        // of that prior edge means we have to return a context with no active token.
        //
        // The other case where we have to return a context with no active token is when
        // we have a current token, but the edge we're traversing is optional and does not exist.
        if self.context.current_token.is_none() || (!self.has_neighbors && self.is_optional_edge) {
            self.recipient
                .do_send(Vertex(self.context.split_and_move_to_token(None)))
        }
    }
}

impl<Token: Debug + Clone + Send + 'static + Unpin> Handler<Node<Token>>
    for EdgeExpanderActor2<Token>
{
    type Result = ();

    fn handle(&mut self, neighbour: Node<Token>, _ctx: &mut Self::Context) -> Self::Result {
        self.has_neighbors = true;
        self.recipient.do_send(Vertex(
            self.context.split_and_move_to_token(Some(neighbour.0)),
        ));
    }
}

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
fn expand_non_recursive_edge<DataToken: Clone + Debug + Send + 'static + Unpin>(
    adapter: Arc<impl ActixAdapter<DataToken = DataToken>>,
    _query: &InterpretedQuery,
    _component: &IRQueryComponent,
    expanding_from: &IRVertex,
    _expanding_to: &IRVertex,
    _edge_id: Eid,
    edge_name: &Arc<str>,
    edge_parameters: &Option<Arc<EdgeParameters>>,
    is_optional: bool,
    recipient: Recipient<Vertex<DataToken>>,
) -> Recipient<Vertex<DataToken>> {
    let recipient1 = adapter.edge_actor(
        expanding_from.type_name.clone(),
        edge_name.clone(),
        edge_parameters.clone(),
    );

    EdgeExpanderActor {
        is_optional_edge: is_optional,
        recipient1,
        recipient2: recipient,
    }
    .start()
    .recipient()
}

/// Apply all the operations needed at entry into a new vertex:
/// - coerce the type, if needed
/// - apply all local filters
/// - record the token at this Vid in the context
#[instrument(skip_all)]
fn perform_entry_into_new_vertex<DataToken: Clone + Debug + Send + 'static + Unpin>(
    adapter: Arc<impl ActixAdapter<DataToken = DataToken>>,
    query: &InterpretedQuery,
    _component: &IRQueryComponent,
    vertex: &IRVertex,
    recipient: Recipient<Vertex<DataToken>>,
) -> Recipient<Vertex<DataToken>> {
    coerce_if_needed(adapter.as_ref(), query, vertex, recipient)
}
