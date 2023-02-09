#![allow(dead_code)]

use std::pin::Pin;
use std::{fs, sync::Arc};

use actix::Actor as _;
use actix::Recipient;
use futures::{stream, Stream, StreamExt};
use git_url_parse::GitUrl;
use hn_api::types::{Item, User};
use octorust::types::{ContentFile, FullRepository};
use trustfall_core::interpreter::{Edge, Node};
use trustfall_core::{
    interpreter::{ActixAdapter, Property, Vertex},
    ir::{EdgeParameters, FieldValue},
};

use crate::token::RepoWorkflow;
use crate::{
    actions_parser::{get_jobs_in_workflow_file, get_steps_in_job},
    token::{Repository, Token},
    util::get_owner_and_repo,
};

const USER_AGENT: &str = "demo-hytradboi (github.com/obi1kenobi/trustfall)";

lazy_static! {
    static ref GITHUB_CLIENT: octorust::Client = octorust::Client::new(
        USER_AGENT,
        Some(octorust::auth::Credentials::Token(
            std::env::var("GITHUB_TOKEN").unwrap_or_else(|_| {
                fs::read_to_string("./localdata/gh_token")
                    .expect("could not find creds file")
                    .trim()
                    .to_string()
            })
        )),
    )
    .unwrap();
    static ref REPOS_CLIENT: octorust::repos::Repos =
        octorust::repos::Repos::new(GITHUB_CLIENT.clone());
}

#[derive(Clone, Copy)]
pub struct DemoAdapter;

impl DemoAdapter {
    pub fn new() -> Self {
        Self
    }

    async fn front_page(self) -> Pin<Box<dyn Stream<Item = Token>>> {
        self.top(Some(30)).await
    }

    async fn get(self, id: u32) -> Option<Token> {
        reqwest::get(format!(
            "https://hacker-news.firebaseio.com/v0/item/{id}.json"
        ))
        .await
        .ok()?
        .json::<Option<Item>>()
        .await
        .ok()
        .flatten()
        .map(Token::from)
    }

    async fn top_stories(&self) -> Vec<u32> {
        reqwest::get("https://hacker-news.firebaseio.com/v0/topstories.json")
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn top(self, max: Option<usize>) -> Pin<Box<dyn Stream<Item = Token> + 'static>> {
        let iterator = stream::iter(self.top_stories().await)
            .take(max.unwrap_or(usize::MAX))
            .filter_map(move |id| self.get(id));

        Box::pin(iterator)
    }

    async fn latest_stories(self, max: Option<usize>) -> Pin<Box<dyn Stream<Item = Token>>> {
        // Unfortunately, the HN crate we're using doesn't support getting the new stories,
        // so we're doing it manually here.
        let story_ids: Vec<u32> =
            reqwest::get("https://hacker-news.firebaseio.com/v0/newstories.json")
                .await
                .unwrap()
                .json()
                .await
                .unwrap();

        let iterator = stream::iter(story_ids)
            .take(max.unwrap_or(usize::MAX))
            .filter_map(move |id| self.get(id));

        Box::pin(iterator)
    }

    async fn user(self, username: &str) -> Option<Token> {
        match reqwest::get(format!(
            "https://hacker-news.firebaseio.com/v0/user/{username}.json"
        ))
        .await
        .unwrap()
        .json::<Option<User>>()
        .await
        .unwrap()
        {
            Some(user) => {
                // Found a user by that name.
                let token = Token::from(user);
                Some(token)
            }
            None => {
                // The request succeeded but did not find a user by that name.
                None
            }
        }
    }
}

// macro_rules! impl_item_property {
//     ($data_contexts:ident, $attr:ident) => {
//         Box::pin($data_contexts.map(|ctx| {
//             let token = ctx.current_token.as_ref();
//             let value = match token {
//                 None => FieldValue::Null,
//                 Some(t) => {
//                     if let Some(s) = t.as_story() {
//                         (&s.$attr).into()
//                     } else if let Some(j) = t.as_job() {
//                         (&j.$attr).into()
//                     } else if let Some(c) = t.as_comment() {
//                         (&c.$attr).into()
//                     } else {
//                         unreachable!()
//                     }
//                 }

//                 #[allow(unreachable_patterns)]
//                 _ => unreachable!(),
//             };

//             (ctx, value)
//         }))
//     };
// }

macro_rules! impl_property {
    ($recipients:ident, $conversion:ident, $attr:ident) => {{
        struct Actor(Recipient<Property<Token>>);
        impl actix::Actor for Actor {
            type Context = actix::Context<Self>;
        }
        impl actix::Handler<Vertex<Token>> for Actor {
            type Result = ();
            #[tracing::instrument("property", skip_all)]
            fn handle(&mut self, msg: Vertex<Token>, _: &mut Self::Context) -> Self::Result {
                let token = msg
                    .0
                    .current_token
                    .as_ref()
                    .map(|token| token.$conversion().unwrap());
                let value = match token {
                    None => FieldValue::Null,
                    Some(t) => (&t.$attr).clone().into(),
                };

                self.0.do_send(Property(msg.0, value));
            }
        }
        Actor($recipients).start().recipient()
    }};

    ($recipients:ident, $conversion:ident, $var:ident, $b:block) => {{
        struct Actor(Recipient<Property<Token>>);
        impl actix::Actor for Actor {
            type Context = actix::Context<Self>;
        }
        impl actix::Handler<Vertex<Token>> for Actor {
            type Result = ();
            #[tracing::instrument("property", skip_all)]
            fn handle(&mut self, msg: Vertex<Token>, _: &mut Self::Context) -> Self::Result {
                let token = msg
                    .0
                    .current_token
                    .as_ref()
                    .map(|token| token.$conversion().unwrap());
                let value = match token {
                    None => FieldValue::Null,
                    Some($var) => $b,
                };

                self.0.do_send(Property(msg.0, value));
            }
        }
        Actor($recipients).start().recipient()
    }};
}

macro_rules! impl_coerce {
    ($recipients:ident, $conversion:ident) => {{
        struct Actor(Recipient<Vertex<Token>>);
        impl actix::Actor for Actor {
            type Context = actix::Context<Self>;
        }
        impl actix::Handler<Vertex<Token>> for Actor {
            type Result = ();
            #[tracing::instrument("coerce", skip_all)]
            fn handle(&mut self, msg: Vertex<Token>, _: &mut Self::Context) -> Self::Result {
                let can_coerce = msg
                    .0
                    .current_token
                    .as_ref()
                    .map(|token| token.$conversion().is_some())
                    .unwrap_or_default();
                if can_coerce {
                    self.0.do_send(msg);
                }
            }
        }
        Actor($recipients).start().recipient()
    }};
}

impl ActixAdapter for DemoAdapter {
    type DataToken = Token;

    fn property_actor(
        &self,
        current_type_name: Arc<str>,
        field_name: Arc<str>,
        recipient: Recipient<Property<Self::DataToken>>,
    ) -> Recipient<Vertex<Self::DataToken>> {
        match (current_type_name.as_ref(), field_name.as_ref()) {
            ("HackerNewsStory", "score") => impl_property!(recipient, as_story, score),

            // properties on Webpage
            ("GitHubRepository", "url") => {
                impl_property!(recipient, as_webpage, url, { url.into() })
            }

            // properties on GitHubWorkflow
            ("GitHubWorkflow", "name") => impl_property!(recipient, as_github_workflow, wf, {
                wf.workflow.name.as_str().into()
            }),
            ("GitHubWorkflow", "path") => impl_property!(recipient, as_github_workflow, wf, {
                wf.workflow.path.as_str().into()
            }),

            // properties on GitHubActionsJob
            ("GitHubActionsJob", "name") => {
                impl_property!(recipient, as_github_actions_job, name)
            }

            // properties on GitHubActionsStep and its implementers
            ("GitHubActionsImportedStep", "name") => {
                impl_property!(recipient, as_github_actions_step, step_name, {
                    step_name.map(|x| x.to_string()).into()
                })
            }

            // properties on GitHubActionsImportedStep
            ("GitHubActionsImportedStep", "uses") => {
                impl_property!(recipient, as_github_actions_imported_step, uses)
            }

            _ => unreachable!(),
        }
    }

    fn edge_actor(
        &self,
        current_type_name: Arc<str>,
        edge_name: Arc<str>,
        parameters: Option<Arc<EdgeParameters>>,
    ) -> Recipient<Edge<Self::DataToken>> {
        match (current_type_name.as_ref(), edge_name.as_ref()) {
            ("@", "HackerNewsTop") => {
                struct Actor(Option<usize>);
                impl actix::Actor for Actor {
                    type Context = actix::Context<Self>;
                }
                impl actix::Handler<Edge<Token>> for Actor {
                    type Result = ();
                    #[tracing::instrument("edge_top", skip_all)]
                    fn handle(&mut self, msg: Edge<Token>, _: &mut Self::Context) -> Self::Result {
                        let max = self.0;
                        let Edge(_, recipient) = msg;
                        actix::spawn(async move {
                            let mut stream = DemoAdapter.top(max).await;
                            while let Some(s) = stream.next().await {
                                recipient.do_send(Node(s));
                            }
                        });
                    }
                }
                let max = parameters
                    .unwrap()
                    .0
                    .get("max")
                    .map(|v| v.as_u64().unwrap() as usize);
                Actor(max).start().recipient()
            }
            ("HackerNewsStory", "link") => {
                struct Actor;
                impl actix::Actor for Actor {
                    type Context = actix::Context<Self>;
                }
                impl actix::Handler<Edge<Token>> for Actor {
                    type Result = ();
                    #[tracing::instrument("edge_link", skip_all)]
                    fn handle(&mut self, msg: Edge<Token>, _: &mut Self::Context) -> Self::Result {
                        let Edge(token, recipient) = msg;
                        if let Some(token) = token.current_token {
                            actix::spawn(async move {
                                let story = token.as_story().unwrap();
                                if let Some(url) = story.url.as_ref() {
                                    if let Some(s) = resolve_url(url.as_str()).await {
                                        recipient.do_send(Node(s));
                                    }
                                }
                            });
                        }
                    }
                }
                Actor.start().recipient()
            }
            ("GitHubRepository", "workflows") => {
                struct Actor;
                impl actix::Actor for Actor {
                    type Context = actix::Context<Self>;
                }
                impl actix::Handler<Edge<Token>> for Actor {
                    type Result = ();
                    #[tracing::instrument("edge_workflows", skip_all)]
                    fn handle(&mut self, msg: Edge<Token>, _: &mut Self::Context) -> Self::Result {
                        let Edge(token, recipient) = msg;
                        if let Some(token) = token.current_token {
                            let per_page = 100;
                            let repo_clone = match token {
                                Token::GitHubRepository(r) => r,
                                _ => unreachable!(),
                            };
                            let client = octorust::actions::Actions::new(GITHUB_CLIENT.clone());
                            actix::spawn(async move {
                                let (owner, repo_name) =
                                    get_owner_and_repo(repo_clone.repo.as_ref());
                                for workflow in client
                                    .list_repo_workflows(owner, repo_name, per_page, 1)
                                    .await
                                    .unwrap()
                                    .workflows
                                {
                                    recipient.do_send(Node(
                                        RepoWorkflow::new(
                                            repo_clone.repo.clone(),
                                            Arc::new(workflow),
                                        )
                                        .into(),
                                    ))
                                }
                            });
                        }
                    }
                }
                Actor.start().recipient()
            }
            ("GitHubWorkflow", "jobs") => {
                struct Actor;
                impl actix::Actor for Actor {
                    type Context = actix::Context<Self>;
                }
                impl actix::Handler<Edge<Token>> for Actor {
                    type Result = ();
                    #[tracing::instrument("jobs", skip_all)]
                    fn handle(&mut self, msg: Edge<Token>, _: &mut Self::Context) -> Self::Result {
                        let Edge(token, recipient) = msg;
                        if let Some(token) = token.current_token {
                            actix::spawn(async move {
                                let workflow = token.as_github_workflow().unwrap();
                                let path = workflow.workflow.path.as_ref();
                                let repo = workflow.repo.as_ref();
                                let workflow_content = get_repo_file_content(repo, path).await;
                                if let Some(content) = workflow_content {
                                    let content = Arc::new(content);
                                    get_jobs_in_workflow_file(content)
                                        .for_each(|x| recipient.do_send(Node(x)));
                                }
                            });
                        }
                    }
                }
                Actor.start().recipient()
            }
            ("GitHubActionsJob", "step") => {
                struct Actor;
                impl actix::Actor for Actor {
                    type Context = actix::Context<Self>;
                }
                impl actix::Handler<Edge<Token>> for Actor {
                    type Result = ();
                    #[tracing::instrument("edge_step", skip_all)]
                    fn handle(&mut self, msg: Edge<Token>, _: &mut Self::Context) -> Self::Result {
                        let Edge(token, recipient) = msg;
                        if let Some(Token::GitHubActionsJob(job)) = token.current_token {
                            get_steps_in_job(job).for_each(|x| recipient.do_send(Node(x)));
                        }
                    }
                }
                Actor.start().recipient()
            }
            _ => unreachable!("{} {}", current_type_name.as_ref(), edge_name.as_ref()),
        }
    }

    fn coerce_actor(
        &self,
        current_type_name: Arc<str>,
        coerce_to_type_name: Arc<str>,
        recipient: Recipient<Vertex<Self::DataToken>>,
    ) -> Recipient<Vertex<Self::DataToken>> {
        match (current_type_name.as_ref(), coerce_to_type_name.as_ref()) {
            ("HackerNewsItem", "HackerNewsStory") => impl_coerce!(recipient, as_story),
            ("Webpage", "GitHubRepository") => impl_coerce!(recipient, as_github_repository),
            ("Repository", "GitHubRepository") => impl_coerce!(recipient, as_github_repository),
            ("GitHubActionsStep", "GitHubActionsImportedStep") => {
                impl_coerce!(recipient, as_github_actions_imported_step)
            }
            unhandled => unreachable!("{:?}", unhandled),
        }
    }
}

async fn resolve_url(url: &str) -> Option<Token> {
    // HACK: Avoiding this bug https://github.com/tjtelan/git-url-parse-rs/issues/22
    if !url.contains("github.com") && !url.contains("gitlab.com") {
        return Some(Token::Webpage(Arc::from(url)));
    }

    let maybe_git_url = GitUrl::parse(url);
    match maybe_git_url {
        Ok(git_url) => {
            if git_url.fullname != git_url.path.trim_matches('/') {
                // The link points *within* the repo rather than *at* the repo.
                // This is just a regular link to a webpage.
                Some(Token::Webpage(Arc::from(url)))
            } else if matches!(git_url.host, Some(x) if x == "github.com") {
                let future = REPOS_CLIENT.get(
                    git_url
                        .owner
                        .as_ref()
                        .unwrap_or_else(|| panic!("repo {url} had no owner"))
                        .as_str(),
                    git_url.name.as_str(),
                );
                match future.await {
                    Ok(repo) => Some(Repository::new(url.to_string(), Arc::new(repo)).into()),
                    Err(e) => {
                        eprintln!("Error getting repository information for url {url}: {e}",);
                        None
                    }
                }
            } else {
                Some(Token::Repository(Arc::from(url)))
            }
        }
        Err(..) => Some(Token::Webpage(Arc::from(url))),
    }
}

async fn get_repo_file_content(repo: &FullRepository, path: &str) -> Option<ContentFile> {
    let (owner, repo_name) = get_owner_and_repo(repo);
    let main_branch = repo.default_branch.as_ref();

    match REPOS_CLIENT
        .get_content_file(owner, repo_name, path, main_branch)
        .await
    {
        Ok(content) => Some(content),
        Err(e) => {
            eprintln!(
                "Error getting repo {owner}/{repo_name} branch {main_branch} file {path}: {e}",
            );
            None
        }
    }
}
