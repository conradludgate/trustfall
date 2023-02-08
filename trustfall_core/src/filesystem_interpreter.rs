#![allow(dead_code)]

use actix::{Actor, Context, Handler, Recipient};
use serde::{Deserialize, Serialize};

use crate::interpreter::{
    ActixAdapter, Adapter, DataContext, Edge, InterpretedQuery, Node, Property, Vertex,
};
use crate::ir::{EdgeParameters, Eid, FieldValue, Vid};
use std::fs::{self, ReadDir};
use std::iter;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct FilesystemInterpreter {
    origin: Rc<String>,
}

impl FilesystemInterpreter {
    pub fn new(origin: String) -> FilesystemInterpreter {
        FilesystemInterpreter {
            origin: Rc::new(origin),
        }
    }
}

#[derive(Debug)]
struct DirectoryContainsFileIterator {
    origin: Rc<String>,
    directory: DirectoryToken,
    file_iter: ReadDir,
}

impl DirectoryContainsFileIterator {
    pub fn new(origin: Rc<String>, directory: &DirectoryToken) -> DirectoryContainsFileIterator {
        let mut buf = PathBuf::new();
        buf.extend([&*origin, &directory.path]);
        DirectoryContainsFileIterator {
            origin,
            directory: directory.clone(),
            file_iter: fs::read_dir(buf).unwrap(),
        }
    }
}

impl Iterator for DirectoryContainsFileIterator {
    type Item = FilesystemToken;

    fn next(&mut self) -> Option<FilesystemToken> {
        loop {
            if let Some(outcome) = self.file_iter.next() {
                match outcome {
                    Ok(dir_entry) => {
                        let metadata = match dir_entry.metadata() {
                            Ok(res) => res,
                            _ => continue,
                        };
                        if metadata.is_file() {
                            let name = dir_entry.file_name().to_str().unwrap().to_owned();
                            let mut buf = PathBuf::new();
                            buf.extend([&self.directory.path, &name]);
                            let extension = Path::new(&name)
                                .extension()
                                .map(|x| x.to_str().unwrap().to_owned());
                            let result = FileToken {
                                name,
                                extension,
                                path: buf.to_str().unwrap().to_owned(),
                            };
                            return Some(FilesystemToken::File(result));
                        }
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}

#[derive(Debug)]
struct SubdirectoryIterator {
    origin: Rc<String>,
    directory: DirectoryToken,
    dir_iter: ReadDir,
}

impl SubdirectoryIterator {
    pub fn new(origin: Rc<String>, directory: &DirectoryToken) -> Self {
        let mut buf = PathBuf::new();
        buf.extend([&*origin, &directory.path]);
        Self {
            origin,
            directory: directory.clone(),
            dir_iter: fs::read_dir(buf).unwrap(),
        }
    }
}

impl Iterator for SubdirectoryIterator {
    type Item = FilesystemToken;

    fn next(&mut self) -> Option<FilesystemToken> {
        loop {
            match self.dir_iter.next()? {
                Ok(dir_entry) => {
                    let metadata = match dir_entry.metadata() {
                        Ok(res) => res,
                        _ => continue,
                    };
                    if metadata.is_dir() {
                        let name = dir_entry.file_name().to_str().unwrap().to_owned();
                        if name == ".git" || name == ".vscode" || name == "target" {
                            continue;
                        };

                        let buf = Path::new(&self.directory.path).join(&name);
                        let result = DirectoryToken {
                            name,
                            path: buf.into_os_string().into_string().unwrap(),
                        };
                        return Some(FilesystemToken::Directory(result));
                    }
                }
                _ => continue,
            }
        }
    }
}

pub type ContextAndValue = (DataContext<FilesystemToken>, FieldValue);

type IndividualEdgeResolver =
    fn(Rc<String>, &FilesystemToken) -> Box<dyn Iterator<Item = FilesystemToken>>;

type IndividualEdgeResolver2 = fn(Rc<String>, FilesystemToken, Recipient<Node<FilesystemToken>>);
type ContextAndIterableOfEdges = (
    DataContext<FilesystemToken>,
    Box<dyn Iterator<Item = FilesystemToken>>,
);

struct ContextIterator {
    origin: Rc<String>,
    contexts: Box<dyn Iterator<Item = DataContext<FilesystemToken>>>,
    edge_resolver: IndividualEdgeResolver,
}

impl ContextIterator {
    pub fn new(
        origin: Rc<String>,
        contexts: Box<dyn Iterator<Item = DataContext<FilesystemToken>>>,
        edge_resolver: IndividualEdgeResolver,
    ) -> Self {
        Self {
            origin,
            contexts,
            edge_resolver,
        }
    }
}

impl Iterator for ContextIterator {
    type Item = (
        DataContext<FilesystemToken>,
        Box<dyn Iterator<Item = FilesystemToken>>,
    );

    fn next(&mut self) -> Option<ContextAndIterableOfEdges> {
        if let Some(context) = self.contexts.next() {
            if let Some(ref token) = &context.current_token {
                let edge_tokens = (self.edge_resolver)(self.origin.clone(), token);
                Some((context, edge_tokens))
            } else {
                let empty_iterator: iter::Empty<FilesystemToken> = iter::empty();
                Some((context, Box::new(empty_iterator)))
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilesystemToken {
    Directory(DirectoryToken),
    File(FileToken),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryToken {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileToken {
    pub name: String,
    pub extension: Option<String>,
    pub path: String,
}

fn directory_contains_file_handler(
    origin: Rc<String>,
    token: &FilesystemToken,
) -> Box<dyn Iterator<Item = FilesystemToken>> {
    let directory_token = match token {
        FilesystemToken::Directory(dir) => dir,
        _ => unreachable!(),
    };
    Box::from(DirectoryContainsFileIterator::new(origin, directory_token))
}

fn directory_subdirectory_handler(
    origin: Rc<String>,
    token: &FilesystemToken,
) -> Box<dyn Iterator<Item = FilesystemToken>> {
    let directory_token = match token {
        FilesystemToken::Directory(dir) => dir,
        _ => unreachable!(),
    };
    Box::from(SubdirectoryIterator::new(origin, directory_token))
}

fn root_directory_handler(
    _origin: Rc<String>,
    _token: FilesystemToken,
    recipient: Recipient<Node<FilesystemToken>>,
) {
    recipient.do_send(Node(FilesystemToken::Directory(DirectoryToken {
        name: "<origin>".to_owned(),
        path: "".to_owned(),
    })));
}

fn directory_contains_file_handler2(
    origin: Rc<String>,
    token: FilesystemToken,
    recipient: Recipient<Node<FilesystemToken>>,
) {
    let directory_token = match token {
        FilesystemToken::Directory(dir) => dir,
        _ => unreachable!(),
    };
    for node in DirectoryContainsFileIterator::new(origin, &directory_token) {
        recipient.do_send(Node(node));
    }
}

fn directory_subdirectory_handler2(
    origin: Rc<String>,
    token: FilesystemToken,
    recipient: Recipient<Node<FilesystemToken>>,
) {
    let directory_token = match token {
        FilesystemToken::Directory(dir) => dir,
        _ => unreachable!(),
    };
    for node in SubdirectoryIterator::new(origin, &directory_token) {
        recipient.do_send(Node(node));
    }
}

#[allow(unused_variables)]
impl Adapter<'static> for FilesystemInterpreter {
    type DataToken = FilesystemToken;

    fn get_starting_tokens(
        &mut self,
        edge: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = FilesystemToken>> {
        assert!(edge.as_ref() == "OriginDirectory");
        assert!(parameters.is_none());
        let token = DirectoryToken {
            name: "<origin>".to_owned(),
            path: "".to_owned(),
        };
        Box::new(iter::once(FilesystemToken::Directory(token)))
    }

    fn project_property(
        &mut self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>>>,
        current_type_name: Arc<str>,
        field_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = ContextAndValue>> {
        match current_type_name.as_ref() {
            "Directory" => match field_name.as_ref() {
                "name" => Box::new(data_contexts.map(|context| match context.current_token {
                    None => (context, FieldValue::Null),
                    Some(FilesystemToken::Directory(ref x)) => {
                        let value = FieldValue::String(x.name.clone());
                        (context, value)
                    }
                    _ => unreachable!(),
                })),
                "path" => Box::new(data_contexts.map(|context| match context.current_token {
                    None => (context, FieldValue::Null),
                    Some(FilesystemToken::Directory(ref x)) => {
                        let value = FieldValue::String(x.path.clone());
                        (context, value)
                    }
                    _ => unreachable!(),
                })),
                _ => todo!(),
            },
            "File" => match field_name.as_ref() {
                "name" => Box::new(data_contexts.map(|context| match context.current_token {
                    None => (context, FieldValue::Null),
                    Some(FilesystemToken::File(ref x)) => {
                        let value = FieldValue::String(x.name.clone());
                        (context, value)
                    }
                    _ => unreachable!(),
                })),
                "path" => Box::new(data_contexts.map(|context| match context.current_token {
                    None => (context, FieldValue::Null),
                    Some(FilesystemToken::File(ref x)) => {
                        let value = FieldValue::String(x.path.clone());
                        (context, value)
                    }
                    _ => unreachable!(),
                })),
                "extension" => Box::new(data_contexts.map(|context| match context.current_token {
                    None => (context, FieldValue::Null),
                    Some(FilesystemToken::File(ref x)) => {
                        let value = x
                            .extension
                            .clone()
                            .map(FieldValue::String)
                            .unwrap_or(FieldValue::Null);
                        (context, value)
                    }
                    _ => unreachable!(),
                })),
                _ => todo!(),
            },
            _ => todo!(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn project_neighbors(
        &mut self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>>>,
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
                Box<dyn Iterator<Item = Self::DataToken>>,
            ),
        >,
    > {
        match (current_type_name.as_ref(), edge_name.as_ref()) {
            ("Directory", "out_Directory_ContainsFile") => {
                let iterator: ContextIterator = ContextIterator::new(
                    self.origin.clone(),
                    data_contexts,
                    directory_contains_file_handler,
                );
                Box::from(iterator)
            }
            ("Directory", "out_Directory_Subdirectory") => {
                let iterator: ContextIterator = ContextIterator::new(
                    self.origin.clone(),
                    data_contexts,
                    directory_subdirectory_handler,
                );
                Box::from(iterator)
            }
            _ => unimplemented!(),
        }
    }

    fn can_coerce_to_type(
        &mut self,
        data_contexts: Box<dyn Iterator<Item = DataContext<Self::DataToken>>>,
        current_type_name: Arc<str>,
        coerce_to_type_name: Arc<str>,
        query_hint: InterpretedQuery,
        vertex_hint: Vid,
    ) -> Box<dyn Iterator<Item = (DataContext<Self::DataToken>, bool)>> {
        todo!()
    }
}

struct EdgeActor {
    origin: Rc<String>,
    edge_resolver: IndividualEdgeResolver2,
}
impl EdgeActor {
    pub fn new(origin: Rc<String>, edge_resolver: IndividualEdgeResolver2) -> Self {
        Self {
            origin,
            edge_resolver,
        }
    }
}

impl Actor for EdgeActor {
    type Context = Context<Self>;
}

impl Handler<Edge<FilesystemToken>> for EdgeActor {
    type Result = ();

    fn handle(&mut self, msg: Edge<FilesystemToken>, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(token) = msg.0.current_token {
            (self.edge_resolver)(self.origin.clone(), token, msg.1)
        }
    }
}

struct RootActor {}
impl RootActor {
    pub fn new() -> Self {
        Self {}
    }
}
impl Actor for RootActor {
    type Context = Context<Self>;
}
impl Handler<Edge<FilesystemToken>> for RootActor {
    type Result = ();

    fn handle(&mut self, msg: Edge<FilesystemToken>, _ctx: &mut Self::Context) -> Self::Result {
        msg.1
            .do_send(Node(FilesystemToken::Directory(DirectoryToken {
                name: "<origin>".to_owned(),
                path: "".to_owned(),
            })));
    }
}

struct PropertyActor<F> {
    f: F,
    recipient: Recipient<Property<FilesystemToken>>,
}

impl<F: for<'a> Fn(&'a FilesystemToken) -> FieldValue + Unpin + 'static> Actor
    for PropertyActor<F>
{
    type Context = Context<Self>;
}

impl<F: for<'a> Fn(&'a FilesystemToken) -> FieldValue + Unpin + 'static>
    Handler<Vertex<FilesystemToken>> for PropertyActor<F>
{
    type Result = ();

    fn handle(&mut self, msg: Vertex<FilesystemToken>, _ctx: &mut Self::Context) -> Self::Result {
        match msg.0.current_token {
            None => self.recipient.do_send(Property(msg.0, FieldValue::Null)),
            Some(ref x) => {
                let value = (self.f)(x);
                self.recipient.do_send(Property(msg.0, value))
            }
        }
    }
}

impl ActixAdapter for FilesystemInterpreter {
    type DataToken = FilesystemToken;

    fn property_actor(
        &self,
        current_type_name: Arc<str>,
        field_name: Arc<str>,
        recipient: Recipient<Property<FilesystemToken>>,
    ) -> Recipient<Vertex<Self::DataToken>> {
        match current_type_name.as_ref() {
            "Directory" => {
                let f = match field_name.as_ref() {
                    "name" => |x: &DirectoryToken| FieldValue::String(x.name.clone()),
                    "path" => |x: &DirectoryToken| FieldValue::String(x.path.clone()),

                    _ => todo!(),
                };
                PropertyActor {
                    recipient,
                    f: move |x: &FilesystemToken| match x {
                        FilesystemToken::Directory(ref x) => f(x),
                        _ => unreachable!(),
                    },
                }
                .start()
                .recipient()
            }
            "File" => {
                let f = match field_name.as_ref() {
                    "name" => |x: &FileToken| FieldValue::String(x.name.clone()),
                    "path" => |x: &FileToken| FieldValue::String(x.path.clone()),
                    "extension" => |x: &FileToken| {
                        x.extension
                            .clone()
                            .map(FieldValue::String)
                            .unwrap_or(FieldValue::Null)
                    },

                    _ => todo!(),
                };
                PropertyActor {
                    recipient,
                    f: move |x: &FilesystemToken| match x {
                        FilesystemToken::File(ref x) => f(x),
                        _ => unreachable!(),
                    },
                }
                .start()
                .recipient()
            }
            _ => todo!(),
        }
    }

    fn edge_actor(
        &self,
        current_type_name: Arc<str>,
        edge_name: Arc<str>,
        _parameters: Option<Arc<EdgeParameters>>,
    ) -> Recipient<Edge<Self::DataToken>> {
        match (current_type_name.as_ref(), edge_name.as_ref()) {
            ("@", "OriginDirectory") => RootActor::new().start().recipient(),
            ("Directory", "out_Directory_ContainsFile") => {
                EdgeActor::new(self.origin.clone(), directory_contains_file_handler2)
                    .start()
                    .recipient()
            }
            ("Directory", "out_Directory_Subdirectory") => {
                EdgeActor::new(self.origin.clone(), directory_subdirectory_handler2)
                    .start()
                    .recipient()
            }
            _ => unimplemented!(),
        }
    }

    fn coerce_actor(
        &self,
        _current_type_name: Arc<str>,
        _coerce_to_type_name: Arc<str>,
        _recipient: Recipient<Vertex<Self::DataToken>>,
    ) -> Recipient<Vertex<Self::DataToken>> {
        todo!()
    }
}
