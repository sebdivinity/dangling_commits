import logging
import shlex
import subprocess as sp
from dataclasses import dataclass
from hashlib import sha1
from urllib.parse import urlparse

from dangling_commits.domain.exceptions import (CommandExecutionError,
                                                GitError, InvalidShaError)


@ dataclass
class LocalObjectsHashes:
    commits: list[str]
    blobs: list[str]
    trees: list[str]
    tags: list[str]


def exec_cmd(cmd: str, exit_on_error: bool = True, stdin: str = "") -> str:
    return exec_cmd_binary(
        cmd=cmd, raise_on_error=exit_on_error, stdin=stdin.encode()).decode(
        "utf-8", errors="replace")


def exec_cmd_binary(cmd: str, raise_on_error: bool = True, stdin: bytes = b'') -> bytes:
    logging.debug("Executing command: %s", cmd)
    with sp.Popen(shlex.split(cmd), stdout=sp.PIPE, stderr=sp.PIPE, stdin=sp.PIPE) as p:
        stdout, stderr = p.communicate(stdin)

    if raise_on_error and p.returncode:
        raise CommandExecutionError(f'Command failed -> {cmd}\nStderr -> {stderr.decode()}')

    return stdout


def calculate_git_sha(data: bytes, object_type: str) -> str:
    s = sha1()
    s.update(f"{object_type} {len(data)}\0".encode())
    s.update(data)
    return s.hexdigest()


def get_local_git_objects() -> LocalObjectsHashes:
    commits: list[str] = []
    trees: list[str] = []
    blobs: list[str] = []
    tags: list[str] = []

    for line in exec_cmd(
            "git cat-file --batch-check --batch-all-objects").rstrip('\n').split("\n"):
        # should happen when no objects exists
        if line == '':
            continue

        sha, object_type = line.split(' ')[:2]
        if object_type == "commit":
            commits.append(sha)
        elif object_type == "tree":
            trees.append(sha)
        elif object_type == "blob":
            blobs.append(sha)
        elif object_type == "tag":
            tags.append(sha)
        else:
            raise GitError(f'Unknown object type: {object_type} for object {sha}')

    return LocalObjectsHashes(commits, blobs, trees, tags)


def get_remote_origin() -> tuple[str, str, str]:
    remote_url = exec_cmd(
        "git remote get-url origin")
    # if ssh url, we convert it to https:// format to use the same parsing
    # method to get required infos
    if "@" in remote_url:
        remote_url = remote_url.split("@", maxsplit=1)[1]
        remote_url = f'https://{remote_url.replace(":", "/", 1)}'

    parsed_url = urlparse(remote_url)

    folder = '/'.join(parsed_url.path.split("/")[:-1]).removeprefix('/')
    repository = parsed_url.path.split("/")[-1].rstrip('\n').removesuffix(".git")

    logging.debug("folder parsed: %s", folder)
    logging.debug("repository parsed: %s", repository)
    logging.debug("server parsed: %s", parsed_url.netloc)

    return parsed_url.netloc, folder, repository


def create_object(data: bytes, object_type: str, original_sha: str) -> None:
    calculated_sha = exec_cmd_binary(
        f'git hash-object --stdin -w -t {object_type}',
        stdin=data).decode().rstrip('\n')

    if calculated_sha != original_sha:
        raise InvalidShaError(
            f"{object_type} created does not have the same sha as the original: {original_sha=} != {calculated_sha=}")
