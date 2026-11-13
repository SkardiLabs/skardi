//! The anchor of the current branch's issue or pull request, for
//! `skardi memory status` (agent memory design, "Anchors", rule 2).
//!
//! The order, first match wins:
//!
//! 1. **An issue number in the branch name** — `1400-audit-export`,
//!    `feat/1400-audit-export`, `fix-1400`, `issue-1400` — on a GitHub
//!    remote: `url:<host>/<owner>/<repo>/issues/1400`. Offline, and the
//!    convention GitHub's own "create a branch" button follows. The remote is
//!    `upstream` when there is one, so a fork's branch names the issue where
//!    issues live, else `origin`.
//! 2. **The branch's pull request**, asked of `gh pr view` when the name gave
//!    nothing and `gh` is installed. A pull request that closes exactly one
//!    issue names that issue, since the issue is the task and the pull
//!    request one step of it; otherwise the pull request's own URL.
//! 3. Otherwise no anchor. `main`, `master`, a detached `HEAD` and the like
//!    never give one, as the design says.
//!
//! Gathering the facts ([`gather`]) is the only part that runs anything;
//! choosing from them ([`from_facts`]) is pure, so the rules are tested
//! without a repository or a network.

use serde_json::Value;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

/// A branch name that never names a task.
const TRUNKS: [&str; 6] = ["main", "master", "develop", "development", "trunk", "HEAD"];

/// The words that may stand before an issue number inside a branch name.
const ISSUE_WORDS: [&str; 8] = [
    "issue", "issues", "gh", "bug", "fix", "fixes", "closes", "ticket",
];

/// How long `gh pr view` may take before status gives up on it: status runs
/// in a hook, before an agent's first turn.
const GH_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one `git` read may take.
const GIT_TIMEOUT: Duration = Duration::from_secs(3);

/// Where a resolved anchor came from, as `status` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorSource {
    /// `status --anchor`.
    Flag,
    /// An issue number in the branch name.
    BranchIssue,
    /// The issue the branch's pull request closes.
    PullRequestIssue,
    /// The branch's pull request itself.
    PullRequest,
}

impl AnchorSource {
    pub fn as_str(self) -> &'static str {
        match self {
            AnchorSource::Flag => "flag",
            AnchorSource::BranchIssue => "branch_issue",
            AnchorSource::PullRequestIssue => "pull_request_issue",
            AnchorSource::PullRequest => "pull_request",
        }
    }
}

/// What status resolved: the anchor, or why there is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub branch: Option<String>,
    /// `None` when nothing applies.
    pub anchor: Option<(String, AnchorSource)>,
}

/// The branch's pull request, as `gh pr view` described it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequest {
    pub url: String,
    /// The URLs of the issues it closes.
    pub closes: Vec<String>,
}

/// What the working directory says about the current task.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Facts {
    pub branch: Option<String>,
    /// The URL of the remote issues are filed under.
    pub remote: Option<String>,
    pub pull_request: Option<PullRequest>,
}

/// Choose the anchor from what was gathered (the module doc's order).
pub fn from_facts(facts: &Facts) -> Resolved {
    let branch = facts.branch.clone();
    let none = Resolved {
        branch: branch.clone(),
        anchor: None,
    };
    let Some(name) = facts.branch.as_deref().filter(|b| !is_trunk(b)) else {
        return none;
    };
    if let (Some(number), Some(repo)) = (
        issue_number(name),
        facts.remote.as_deref().and_then(github_repo),
    ) {
        return Resolved {
            branch,
            anchor: Some((
                format!("url:{repo}/issues/{number}"),
                AnchorSource::BranchIssue,
            )),
        };
    }
    if let Some(pr) = &facts.pull_request {
        let anchor = match pr.closes.as_slice() {
            [issue] => (format!("url:{issue}"), AnchorSource::PullRequestIssue),
            _ => (format!("url:{}", pr.url), AnchorSource::PullRequest),
        };
        return Resolved {
            branch,
            anchor: Some(anchor),
        };
    }
    none
}

fn is_trunk(branch: &str) -> bool {
    TRUNKS.contains(&branch)
}

/// The issue number a branch name carries, read from its last path segment:
/// a leading number (`1400-audit-export`), or a number right after one of
/// [`ISSUE_WORDS`] (`fix-1400`, `issue_1400`). A number anywhere else — a
/// date, a version — is not taken for an issue.
pub fn issue_number(branch: &str) -> Option<u64> {
    let segment = branch.rsplit('/').next().unwrap_or(branch);
    let tokens: Vec<&str> = segment.split(['-', '_']).collect();
    let number = |token: &str| -> Option<u64> {
        let digits = token.strip_prefix('#').unwrap_or(token);
        if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok().filter(|n| *n > 0)
    };
    if let Some(n) = tokens.first().and_then(|t| number(t)) {
        return Some(n);
    }
    tokens.windows(2).find_map(|pair| {
        ISSUE_WORDS
            .contains(&pair[0].to_ascii_lowercase().as_str())
            .then(|| number(pair[1]))
            .flatten()
    })
}

/// `<host>/<owner>/<repo>` for a GitHub remote URL, in any of git's
/// spellings (`https://`, `ssh://`, `git@host:owner/repo`), with `.git` and
/// credentials dropped. `None` for a host that is not GitHub, whose issue
/// URLs this does not know.
pub fn github_repo(remote: &str) -> Option<String> {
    let remote = remote.trim();
    let rest = match remote.split_once("://") {
        Some((_, rest)) => rest.to_string(),
        // scp-like: `git@github.com:owner/repo.git`.
        None => remote.replacen(':', "/", 1),
    };
    let rest = rest
        .rsplit_once('@')
        .map_or(rest.as_str(), |(_, after)| after);
    let mut parts = rest.split('/').filter(|p| !p.is_empty());
    let host = parts.next()?.to_ascii_lowercase();
    // An `ssh://host:22/…` port is not part of the URL that names an issue.
    let host = host.split(':').next().unwrap_or(&host).to_string();
    let host = host.strip_prefix("www.").unwrap_or(&host).to_string();
    if !(host == "github.com" || host.starts_with("github.")) {
        return None;
    }
    let owner = parts.next()?;
    let repo = parts.next()?;
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    if repo.is_empty() || parts.next().is_some() {
        return None;
    }
    Some(format!("{host}/{owner}/{repo}"))
}

/// Read the facts from the repository at `dir`. Every failure — not a
/// repository, no remote, no `gh`, `gh` not signed in, a timeout — leaves
/// its fact empty: status then reports that no anchor applies, which is
/// true for this directory.
pub async fn gather(dir: &Path) -> Facts {
    let branch = git(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).await;
    let mut remote = git(dir, &["remote", "get-url", "upstream"]).await;
    if remote.is_none() {
        remote = git(dir, &["remote", "get-url", "origin"]).await;
    }
    let mut facts = Facts {
        branch,
        remote,
        pull_request: None,
    };
    // `gh` is a network round trip, so it is asked only when the branch name
    // gave no anchor and there is a branch to ask about.
    let needs_gh = from_facts(&facts).anchor.is_none()
        && facts.branch.as_deref().is_some_and(|b| !is_trunk(b));
    if needs_gh {
        facts.pull_request = gh_pull_request(dir).await;
    }
    facts
}

async fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let mut command = Command::new("git");
    command.arg("-C").arg(dir).args(args);
    run(command, GIT_TIMEOUT).await
}

async fn gh_pull_request(dir: &Path) -> Option<PullRequest> {
    let mut command = Command::new("gh");
    command
        .current_dir(dir)
        .args(["pr", "view", "--json", "url,closingIssuesReferences"]);
    let out = run(command, GH_TIMEOUT).await?;
    parse_gh_pr(&out)
}

/// `gh pr view --json url,closingIssuesReferences`, read.
pub fn parse_gh_pr(text: &str) -> Option<PullRequest> {
    let value: Value = serde_json::from_str(text).ok()?;
    let url = value.get("url")?.as_str()?.to_string();
    let closes = value
        .get("closingIssuesReferences")
        .and_then(Value::as_array)
        .map(|issues| {
            issues
                .iter()
                .filter_map(|issue| issue.get("url").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Some(PullRequest { url, closes })
}

/// Run `command`, returning its trimmed stdout when it exits 0 within
/// `timeout`. Its stderr is discarded: a hook's output is the agent's
/// context, and `git`'s "not a git repository" is noise there.
async fn run(mut command: Command, timeout: Duration) -> Option<String> {
    command
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(timeout, command.output())
        .await
        .ok()?
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::process::Command as StdCommand;

    #[rstest]
    #[case::leading("1400-audit-export", Some(1400))]
    #[case::leading_in_path("feat/1400-audit-export", Some(1400))]
    #[case::bare("1400", Some(1400))]
    #[case::after_fix("fix-1400", Some(1400))]
    #[case::after_issue_underscore("issue_77_null_rows", Some(77))]
    #[case::hash("gh-#12", Some(12))]
    #[case::no_number("feat/memory-gateway", None)]
    #[case::hex_suffix("claude/angry-benz-2bc725", None)]
    #[case::date("release-2026-11", None)]
    #[case::version("deps/serde-1.0.200", None)]
    #[case::zero("0-scratch", None)]
    fn an_issue_number_is_read_only_where_a_branch_puts_one(
        #[case] branch: &str,
        #[case] expected: Option<u64>,
    ) {
        assert_eq!(issue_number(branch), expected);
    }

    #[rstest]
    #[case::https(
        "https://github.com/SkardiLabs/skardi.git",
        Some("github.com/SkardiLabs/skardi")
    )]
    #[case::scp(
        "git@github.com:SkardiLabs/skardi.git",
        Some("github.com/SkardiLabs/skardi")
    )]
    #[case::ssh(
        "ssh://git@github.com:22/SkardiLabs/skardi",
        Some("github.com/SkardiLabs/skardi")
    )]
    #[case::credentials(
        "https://x:tok@GitHub.com/SkardiLabs/skardi/",
        Some("github.com/SkardiLabs/skardi")
    )]
    #[case::enterprise("https://github.acme.io/eng/api.git", Some("github.acme.io/eng/api"))]
    #[case::gitlab("https://gitlab.com/acme/api.git", None)]
    #[case::too_deep("https://github.com/a/b/c", None)]
    fn a_github_remote_is_read_in_every_spelling(
        #[case] remote: &str,
        #[case] expected: Option<&str>,
    ) {
        assert_eq!(github_repo(remote).as_deref(), expected);
    }

    fn facts(branch: &str, pr: Option<PullRequest>) -> Facts {
        Facts {
            branch: Some(branch.to_string()),
            remote: Some("git@github.com:SkardiLabs/skardi-cloud.git".to_string()),
            pull_request: pr,
        }
    }

    #[test]
    fn the_branch_name_wins_over_the_pull_request() {
        let pr = PullRequest {
            url: "https://github.com/SkardiLabs/skardi-cloud/pull/1412".to_string(),
            closes: vec![],
        };
        let resolved = from_facts(&facts("1400-audit-export", Some(pr)));
        assert_eq!(
            resolved.anchor,
            Some((
                "url:github.com/SkardiLabs/skardi-cloud/issues/1400".to_string(),
                AnchorSource::BranchIssue
            ))
        );
    }

    #[rstest]
    #[case::closes_one(
        vec!["https://github.com/SkardiLabs/skardi-cloud/issues/1400"],
        "url:https://github.com/SkardiLabs/skardi-cloud/issues/1400",
        AnchorSource::PullRequestIssue
    )]
    #[case::closes_none(
        vec![],
        "url:https://github.com/SkardiLabs/skardi-cloud/pull/1412",
        AnchorSource::PullRequest
    )]
    #[case::closes_two(
        vec!["https://github.com/a/b/issues/1", "https://github.com/a/b/issues/2"],
        "url:https://github.com/SkardiLabs/skardi-cloud/pull/1412",
        AnchorSource::PullRequest
    )]
    fn a_pull_request_names_the_one_issue_it_closes_or_itself(
        #[case] closes: Vec<&str>,
        #[case] anchor: &str,
        #[case] source: AnchorSource,
    ) {
        let pr = PullRequest {
            url: "https://github.com/SkardiLabs/skardi-cloud/pull/1412".to_string(),
            closes: closes.into_iter().map(str::to_string).collect(),
        };
        let resolved = from_facts(&facts("feat/audit-export", Some(pr)));
        assert_eq!(resolved.anchor, Some((anchor.to_string(), source)));
    }

    #[rstest]
    #[case::main("main")]
    #[case::detached("HEAD")]
    #[case::unnumbered("feat/audit-export")]
    fn a_trunk_or_an_unlinked_branch_gives_no_anchor(#[case] branch: &str) {
        let pr = PullRequest {
            url: "https://github.com/SkardiLabs/skardi-cloud/pull/1".to_string(),
            closes: vec![],
        };
        let with_pr = if branch == "feat/audit-export" {
            None
        } else {
            Some(pr)
        };
        let resolved = from_facts(&facts(branch, with_pr));
        assert_eq!(resolved.anchor, None);
        assert_eq!(resolved.branch.as_deref(), Some(branch));
    }

    #[test]
    fn gh_output_is_read() {
        let pr = parse_gh_pr(
            r#"{"url":"https://github.com/a/b/pull/9","closingIssuesReferences":[{"number":3,"url":"https://github.com/a/b/issues/3"}]}"#,
        )
        .unwrap();
        assert_eq!(pr.url, "https://github.com/a/b/pull/9");
        assert_eq!(pr.closes, vec!["https://github.com/a/b/issues/3"]);
        assert_eq!(parse_gh_pr("no pull requests found"), None);
    }

    /// The real gatherer against a throwaway repository: the branch and the
    /// remote are read, and a numbered branch never reaches `gh`.
    #[tokio::test]
    async fn gather_reads_a_real_repository() {
        let dir = tempfile::TempDir::new().unwrap();
        let git = |args: &[&str]| {
            let status = StdCommand::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        git(&["init", "-q", "-b", "1400-audit-export"]);
        git(&[
            "remote",
            "add",
            "origin",
            "https://github.com/SkardiLabs/skardi-cloud.git",
        ]);
        git(&[
            "-c",
            "user.email=t@example.com",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "init",
        ]);
        let facts = gather(dir.path()).await;
        assert_eq!(facts.branch.as_deref(), Some("1400-audit-export"));
        assert_eq!(facts.pull_request, None);
        assert_eq!(
            from_facts(&facts).anchor.map(|(a, _)| a).as_deref(),
            Some("url:github.com/SkardiLabs/skardi-cloud/issues/1400")
        );

        // Not a repository: no facts, no anchor, no error.
        let empty = tempfile::TempDir::new().unwrap();
        let facts = gather(empty.path()).await;
        assert_eq!(facts.branch, None);
        assert_eq!(from_facts(&facts).anchor, None);
    }
}
