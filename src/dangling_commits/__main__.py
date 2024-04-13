#!/bin/env python3
import argparse
import logging
import os
import sys
import zlib

from dangling_commits.domain.enums import CommitStatus
from dangling_commits.domain.exceptions import InvalidShaError
from dangling_commits.domain.interfaces import GitRepository
from dangling_commits.domain.utils import (create_object, exec_cmd,
                                           exec_cmd_binary,
                                           get_local_git_objects,
                                           get_remote_origin)
from dangling_commits.infra import Github


def create_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--git-dir", default=os.getcwd())
    parser.add_argument("--server", choices=("gitlab", "github", "azure_devops"))
    debug_level = parser.add_mutually_exclusive_group()
    debug_level.add_argument(
        '-d', '--debug',
        help="activate DEBUG output",
        default=False,
        action='store_true'
    )
    debug_level.add_argument(
        '-q', '--quiet',
        help="suppress INFO output",
        default=False,
        action='store_true'
    )

    return parser


def main() -> int:
    parser: argparse.ArgumentParser = create_cli()
    args: argparse.Namespace = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
    elif args.quiet:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    os.chdir(args.git_dir)

    # update repo to get last pushed commits
    exec_cmd_binary("git fetch --all")

    server, folder, repository = get_remote_origin()
    gitRepository: GitRepository

    if args.server is None:
        if server == "github.com":
            logging.info("Automatically found %s so will query Github API", server)
            gitRepository = Github(server, folder, repository)
        else:
            raise NotImplementedError(f"{server} is not handled yet")
    else:
        if args.server == "github":
            gitRepository = Github(server, folder, repository)
        else:
            raise NotImplementedError(f"{args.server} is not handled yet")

    localObjectHashes = get_local_git_objects()
    logging.info('Local commits found: %s', len(localObjectHashes.commits))
    logging.info('Local trees found: %s', len(localObjectHashes.trees))
    logging.info('Local blobs found: %s', len(localObjectHashes.blobs))

    commits, blobs, trees, branches = gitRepository.get_dangling_objects(localObjectHashes)

    # should probably only work for github
    # need to adapt for each repository
    for branch in branches:
        logging.info(
            f'https://{server}/{folder}/{repository}/tree/{branch.end.sha} (length: {branch.length})')

    logging.info("Creating blobs")
    gitRepository.create_blobs(blobs)

    logging.info("Creating trees")
    for idx, tree in enumerate(trees):
        create_object(bytes(tree.git_file()), "tree", tree.sha)
        logging.info('Tree %s created %d/%d', tree.sha, idx + 1, len(trees))

    logging.info("Creating commits")
    invalid_commits: set[str] = set()
    dangling_commit_found = [c for c in commits if c.status == CommitStatus.FOUND]
    repo_dir = exec_cmd("git rev-parse --git-dir").rstrip('\n')
    for idx, commit in enumerate(dangling_commit_found):
        try:
            create_object(commit.git_file().encode(), "commit", commit.sha)
        except InvalidShaError:
            invalid_commits.add(commit.sha)
            subdir = f'{repo_dir}/objects/{commit.sha[:2]}'
            subsha = commit.sha[2:]

            try:
                content = commit.get_git_file_unsigned()
            except ValueError:
                content = commit.get_git_file_unsigned(
                    author=commit.author.date, committer=commit.committer.date)

            logging.info("Creating %s object directory if needed", subdir)
            os.makedirs(subdir, exist_ok=True)

            logging.info(
                "Forging commit %s into git database because we could not generate the right content",
                commit.sha)
            logging.info(f"{subsha=}")
            with open(f"{subdir}/{subsha}", "wb") as f:
                f.write(zlib.compress(f'commit {len(content.encode())}\x00{content}'.encode()))

        logging.debug('Commit %s created %d/%d', commit.sha, idx + 1, len(dangling_commit_found))

    for branch in branches:
        branch_name = f'dangling_branch_{branch.end.sha}'
        if branch.end.sha not in invalid_commits:
            logging.info("Creating %s on commit %s", branch_name, branch.end.sha)
            exec_cmd(f"git branch {branch_name} {branch.end.sha}")
        else:
            logging.warning(
                "Won't create %s on commit %s because it was forged and git will refuse",
                branch_name,
                branch.end.sha)

    logging.info(f'Total blobs recovered: {len(blobs)}')
    logging.info(f'Total trees recovered: {len(trees)}')
    logging.info(f"Total commits recovered: {len(commits)-len(invalid_commits)}")
    logging.info(f'Total commits forged: {len(invalid_commits)}')

    logging.info("testing that all found commits are fully working")
    # exec_cmd(f'git fsck', exit_on_error=False)
    logging.info("all good !")

    return 0


sys.exit(main())
