use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor as _, System};
use async_adapter::DemoAdapter;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;
use trustfall_core::interpreter::actor_execution::Output;
use trustfall_core::ir::TransparentValue;
use trustfall_core::{
    frontend::parse, interpreter::actor_execution::interpret_ir, ir::FieldValue, schema::Schema,
};

use tracing_subscriber::{prelude::*, Registry};

#[macro_use]
extern crate lazy_static;

mod actions_parser;
mod adapter;
mod async_adapter;
mod pagers;
mod token;
mod util;

lazy_static! {
    static ref SCHEMA: Schema =
        Schema::parse(fs::read_to_string("./schema.graphql").unwrap()).unwrap();
}

#[derive(Debug, Clone, Deserialize)]
struct InputQuery<'a> {
    query: &'a str,

    args: Arc<BTreeMap<Arc<str>, FieldValue>>,
}

#[actix_rt::main]
async fn execute_query(path: &str) {
    let content = fs::read_to_string(path).unwrap();
    let input_query: InputQuery = ron::from_str(&content).unwrap();

    let adapter = Arc::new(DemoAdapter::new());

    let query = parse(&SCHEMA, input_query.query).unwrap();
    let arguments = input_query.args;

    // let max_results = 20usize;

    struct Actor;
    impl actix::Actor for Actor {
        type Context = actix::Context<Self>;
    }
    impl actix::Handler<Output> for Actor {
        type Result = ();
        fn handle(&mut self, msg: Output, _: &mut Self::Context) -> Self::Result {
            let data_item: BTreeMap<Arc<str>, TransparentValue> =
                msg.0.into_iter().map(|(k, v)| (k, v.into())).collect();

            println!(
                "\nResult  fetched, {}",
                serde_json::to_string_pretty(&data_item).unwrap()
            );
        }
    }
    let actor = Actor.start();
    interpret_ir(adapter, query, arguments, actor.recipient());

    actix_rt::time::sleep(Duration::from_secs(10)).await;

    System::current().stop()
}

fn main() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("trustfall_core=debug,demo_hytradboi=debug,warn"));

    let formatting_layer = tracing_tree::HierarchicalLayer::default()
        .with_writer(tracing_subscriber::fmt::TestWriter::new())
        .with_indent_lines(true)
        .with_ansi(true)
        .with_targets(true)
        .with_indent_amount(2);

    let subscriber = Registry::default().with(env_filter).with(formatting_layer);
    subscriber.init();

    let args: Vec<String> = env::args().collect();
    let mut reversed_args: Vec<_> = args.iter().map(|x| x.as_str()).rev().collect();

    reversed_args
        .pop()
        .expect("Expected the executable name to be the first argument, but was missing");

    match reversed_args.pop() {
        None => panic!("No command given"),
        Some("query") => match reversed_args.pop() {
            None => panic!("No filename provided"),
            Some(path) => {
                dbg!(path);
                assert!(reversed_args.is_empty());
                execute_query(path)
            }
        },
        Some(cmd) => panic!("Unrecognized command given: {cmd}"),
    }
}
