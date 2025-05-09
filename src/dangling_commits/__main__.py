#!/bin/env python3
import argparse
import json
import logging
import os
import sys
import zlib
from pprint import pp

from dangling_commits.domain.enums import CommitStatus
from dangling_commits.domain.exceptions import InvalidShaError
from dangling_commits.domain.git_objects.commit import Commit
from dangling_commits.domain.interfaces import GitRepository
from dangling_commits.domain.utils import (calculate_git_sha, create_object,
                                           exec_cmd, exec_cmd_binary,
                                           get_local_git_objects,
                                           get_remote_origin)
from dangling_commits.infra import Github, Gitlab


def create_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--git-dir", default=os.getcwd())
    parser.add_argument("--server", choices=("gitlab", "github", "azure_devops"))
    parser.add_argument(
        "--save", action="store_true",
        help="Create a json file containing hashes of dangling objects retrieved")
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
            raise NotImplementedError(
                f"{server} cannot be automatically handled yet, maybe try using --server argument to manually specify it")
    else:
        if args.server == "github":
            gitRepository = Github(server, folder, repository)
        elif args.server == "gitlab":
            gitRepository = Gitlab(server, folder, repository)
        else:
            raise NotImplementedError(f"{args.server} is not handled yet")

    localObjectHashes = get_local_git_objects()
    logging.info('Local commits found: %s', len(localObjectHashes.commits))
    logging.info('Local trees found: %s', len(localObjectHashes.trees))
    logging.info('Local blobs found: %s', len(localObjectHashes.blobs))
    logging.info('Local tags found: %s', len(localObjectHashes.tags))

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
            # logging.info(f"{subsha=}")
            with open(f"{subdir}/{subsha}", "wb") as f:
                f.write(zlib.compress(f'commit {len(content.encode())}\x00{content}'.encode()))

        logging.debug('Commit %s created %d/%d', commit.sha, idx + 1, len(dangling_commit_found))

    # pp(invalid_commits)

    logging.info("Creating branches pointing on head of dangling trees")

    for branch in branches:
        branch_name = f'dangling_branch_{branch.end.sha}'
        if branch.end.sha not in invalid_commits:
            logging.info("Creating %s on commit %s", branch_name, branch.end.sha)
            exec_cmd(f"git branch {branch_name} {branch.end.sha}")
        else:
            # can't create a branch on forged commit
            # create a valid commit which will replace the forged commit
            # the branch is pointing at the valid commit
            # this way, things seems to work for git
            c = [c for c in dangling_commit_found if branch.end.sha == c.sha][0]
            valid_commit = Commit(
                sha="0",
                status=c.status,
                children=set(),
                author=c.author,
                committer=c.committer,
                message=f'VALID COMMIT CREATED BECAUSE {branch.end.sha} IS FORGED:\n{c.message}',
                signature=None,
                tree=c.tree,
                parents=c.parents)
            # this is the same thing done when calling get_git_file_unsigned to forge commit
            # I don't remember why I pass date directly to get_git_file_unsigned to
            # overcome date format error, it prevents str(self.author) to fail
            try:
                data = valid_commit.get_git_file_unsigned().encode()
            except ValueError:
                data = valid_commit.get_git_file_unsigned(
                    author=valid_commit.author.date,
                    committer=valid_commit.committer.date).encode()
            valid_commit.sha = calculate_git_sha(data, "commit")
            # print(f'{valid_commit.sha=}')
            create_object(data, "commit", calculate_git_sha(data, "commit"))
            logging.warning(
                "Won't create %s on commit %s because it was forged and git will refuse",
                branch_name,
                branch.end.sha)
            logging.info("Creating %s on commit %s", branch_name, valid_commit.sha)
            exec_cmd(f"git branch {branch_name} {valid_commit.sha}")

    logging.info(f'Total blobs recovered: {len(blobs)}')
    logging.info(f'Total trees recovered: {len(trees)}')
    logging.info(f"Total commits recovered: {len(commits)-len(invalid_commits)}")
    logging.info(f'Total commits forged: {len(invalid_commits)}')

    if args.save is True and (trees or blobs or dangling_commit_found):
        dangling_objects: dict[str, list[str]] = {
            "commits": [c.sha for c in dangling_commit_found],
            "trees": [t.sha for t in trees],
            "blobs": [b.sha for b in blobs]
        }

        # FIXME: parse realpath
        # filepath = f'{args.git_dir}/dangling_objects.json'
        filepath = 'dangling_objects.json'
        with open(filepath, 'w') as f:
            json.dump(dangling_objects, f)
        logging.info(f"Dangling objects hashes saved in {filepath}")

    logging.info("testing that all found commits are fully working")
    # exec_cmd(f'git fsck', exit_on_error=False)
    logging.info("all good !")

    return 0


sys.exit(main())
