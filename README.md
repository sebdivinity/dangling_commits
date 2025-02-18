# Dangling commits

For context of why this tool exists and what it does, read this article: https://medium.com/etticblog/hunting-erased-secrets-in-dangling-commits-9d090210ee1e

## Installation

This project only uses native libraries of python. You don't need to use pipx or a virtual environnment to ease the dependency management.
However, the Github implementation relies on the tool `gh`. You need to install and configure it (login with your account).

```
git clone https://github.com/sebdivinity/dangling_commits
pip install .
dangling_commits -h
```

or directly install remote repository

```
pip install git+https://github.com/sebdivinity/dangling_commits
dangling_commits -h
```

### Github setup

1. Install `gh` (https://cli.github.com/)
2. Authenticate with it, `gh auth login`

Otherwise, you will have errors when using the tool on a repository cloned from Github.

### External dependencies

Right now, this project uses some external programs. Some of them only existing on Linux. The end goal is to only use pure python modules.

- The github implementation to retrieve dangling commits use the cli tool `gh`. Could be improved to only use python http requests.
- It also uses `base64` tool on Linux. Could be easily improved by using native base64 python.

## Improvement idea

- Retrieve original names of delete branches (which can be seen in activity/events and could probably work searching PRs aswell)
- investigate forks
- BUG: failed to overcome ^V, why ?
- Github : investigate if /events can contains more things than activity in some cases (events don't show anything older than 90 days)
- after getting all dangling commits, check patch files ?
