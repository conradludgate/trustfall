use std::sync::Arc;

use consecrates::api::Crate;
use hn_api::types::{Comment, Item, Job, Story, User};
use octorust::types::{FullRepository, Workflow};
use yaml_rust::Yaml;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Token {
    HackerNewsItem(Arc<Item>),
    HackerNewsStory(Arc<Story>),
    HackerNewsJob(Arc<Job>),
    HackerNewsComment(Arc<Comment>),
    HackerNewsUser(Arc<User>),
    Crate(Arc<Crate>),
    Repository(Arc<str>),
    GitHubRepository(Arc<Repository>),
    GitHubWorkflow(Arc<RepoWorkflow>),
    GitHubActionsJob(Arc<ActionsJob>),
    GitHubActionsImportedStep(Arc<ActionsImportedStep>),
    GitHubActionsRunStep(Arc<ActionsRunStep>),
    NameValuePair(Arc<(String, String)>),
    Webpage(Arc<str>),
}

#[derive(Debug, Clone)]
pub struct Repository {
    pub url: String,
    pub repo: Arc<FullRepository>,
}

impl Repository {
    pub(crate) fn new(url: String, repo: Arc<FullRepository>) -> Self {
        Self { url, repo }
    }
}

#[derive(Debug, Clone)]
pub struct RepoWorkflow {
    pub repo: Arc<FullRepository>,
    pub workflow: Arc<Workflow>,
}

impl RepoWorkflow {
    pub(crate) fn new(repo: Arc<FullRepository>, workflow: Arc<Workflow>) -> Self {
        Self { repo, workflow }
    }
}

#[derive(Debug, Clone)]
pub struct ActionsJob {
    pub yaml: Yaml,
    pub name: String,
    pub runs_on: Option<String>,
}

impl ActionsJob {
    pub(crate) fn new(yaml: Yaml, name: String, runs_on: Option<String>) -> Self {
        Self {
            yaml,
            name,
            runs_on,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActionsImportedStep {
    pub yaml: Yaml,
    pub name: Option<String>,
    pub uses: String,
}

impl ActionsImportedStep {
    pub(crate) fn new(yaml: Yaml, name: Option<String>, uses: String) -> Self {
        Self { yaml, name, uses }
    }
}

#[derive(Debug, Clone)]
pub struct ActionsRunStep {
    pub yaml: Yaml,
    pub name: Option<String>,
    pub run: Vec<String>,
}

impl ActionsRunStep {
    pub(crate) fn new(yaml: Yaml, name: Option<String>, run: Vec<String>) -> Self {
        Self { yaml, name, run }
    }
}

#[allow(dead_code)]
impl Token {
    pub fn typename(&self) -> &'static str {
        match self {
            Token::HackerNewsItem(..) => "HackerNewsItem",
            Token::HackerNewsStory(..) => "HackerNewsStory",
            Token::HackerNewsJob(..) => "HackerNewsJob",
            Token::HackerNewsComment(..) => "HackerNewsComment",
            Token::HackerNewsUser(..) => "HackerNewsUser",
            Token::Crate(..) => "Crate",
            Token::Repository(..) => "Repository",
            Token::GitHubRepository(..) => "GitHubRepository",
            Token::GitHubWorkflow(..) => "GitHubWorkflow",
            Token::GitHubActionsJob(..) => "GitHubActionsJob",
            Token::GitHubActionsImportedStep(..) => "GitHubActionsImportedStep",
            Token::GitHubActionsRunStep(..) => "GitHubActionsRunStep",
            Token::NameValuePair(..) => "NameValuePair",
            Token::Webpage(..) => "Webpage",
        }
    }

    pub fn as_story(&self) -> Option<&Story> {
        match self {
            Token::HackerNewsStory(s) => Some(s.as_ref()),
            Token::HackerNewsItem(i) => match &**i {
                Item::Story(s) => Some(s),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_job(&self) -> Option<&Job> {
        match self {
            Token::HackerNewsJob(s) => Some(s.as_ref()),
            Token::HackerNewsItem(i) => match &**i {
                Item::Job(s) => Some(s),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_comment(&self) -> Option<&Comment> {
        match self {
            Token::HackerNewsComment(s) => Some(s.as_ref()),
            Token::HackerNewsItem(i) => match &**i {
                Item::Comment(s) => Some(s),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_user(&self) -> Option<&User> {
        match self {
            Token::HackerNewsUser(u) => Some(u.as_ref()),
            _ => None,
        }
    }

    pub fn as_crate(&self) -> Option<&Crate> {
        match self {
            Token::Crate(c) => Some(c.as_ref()),
            _ => None,
        }
    }

    pub fn as_webpage(&self) -> Option<&str> {
        match self {
            Token::GitHubRepository(r) => Some(r.url.as_ref()),
            Token::Repository(r) => Some(r.as_ref()),
            Token::Webpage(w) => Some(w.as_ref()),
            _ => None,
        }
    }

    pub fn as_repository(&self) -> Option<&str> {
        match self {
            Token::GitHubRepository(r) => Some(r.url.as_ref()),
            Token::Repository(r) => Some(r.as_ref()),
            _ => None,
        }
    }

    pub fn as_github_repository(&self) -> Option<&FullRepository> {
        match self {
            Token::GitHubRepository(r) => Some(r.repo.as_ref()),
            _ => None,
        }
    }

    pub fn as_github_workflow(&self) -> Option<&RepoWorkflow> {
        match self {
            Token::GitHubWorkflow(w) => Some(w.as_ref()),
            _ => None,
        }
    }

    pub fn as_github_actions_job(&self) -> Option<&ActionsJob> {
        match self {
            Token::GitHubActionsJob(j) => Some(j.as_ref()),
            _ => None,
        }
    }

    pub fn as_github_actions_step(&self) -> Option<Option<&str>> {
        match self {
            Token::GitHubActionsImportedStep(imp) => Some(imp.name.as_deref()),
            Token::GitHubActionsRunStep(r) => Some(r.name.as_deref()),
            _ => None,
        }
    }

    pub fn as_github_actions_run_step(&self) -> Option<&ActionsRunStep> {
        match self {
            Token::GitHubActionsRunStep(r) => Some(r.as_ref()),
            _ => None,
        }
    }

    pub fn as_github_actions_imported_step(&self) -> Option<&ActionsImportedStep> {
        match self {
            Token::GitHubActionsImportedStep(imp) => Some(imp.as_ref()),
            _ => None,
        }
    }

    pub fn as_name_value_pair(&self) -> Option<&(String, String)> {
        match self {
            Token::NameValuePair(nvp) => Some(nvp.as_ref()),
            _ => None,
        }
    }
}

impl From<Item> for Token {
    fn from(item: Item) -> Self {
        Self::HackerNewsItem(Arc::from(item))
    }
}

impl From<Story> for Token {
    fn from(s: Story) -> Self {
        Self::HackerNewsStory(Arc::from(s))
    }
}

impl From<Job> for Token {
    fn from(j: Job) -> Self {
        Self::HackerNewsJob(Arc::from(j))
    }
}

impl From<Comment> for Token {
    fn from(c: Comment) -> Self {
        Self::HackerNewsComment(Arc::from(c))
    }
}

impl From<User> for Token {
    fn from(u: User) -> Self {
        Self::HackerNewsUser(Arc::from(u))
    }
}

impl From<Crate> for Token {
    fn from(c: Crate) -> Self {
        Self::Crate(Arc::from(c))
    }
}

impl From<Repository> for Token {
    fn from(r: Repository) -> Self {
        Self::GitHubRepository(Arc::from(r))
    }
}

impl From<RepoWorkflow> for Token {
    fn from(w: RepoWorkflow) -> Self {
        Self::GitHubWorkflow(Arc::from(w))
    }
}

impl From<ActionsJob> for Token {
    fn from(j: ActionsJob) -> Self {
        Self::GitHubActionsJob(Arc::from(j))
    }
}

impl From<ActionsImportedStep> for Token {
    fn from(imp: ActionsImportedStep) -> Self {
        Self::GitHubActionsImportedStep(Arc::from(imp))
    }
}

impl From<ActionsRunStep> for Token {
    fn from(r: ActionsRunStep) -> Self {
        Self::GitHubActionsRunStep(Arc::from(r))
    }
}
