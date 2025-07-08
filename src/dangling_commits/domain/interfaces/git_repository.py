import logging

from dangling_commits.domain.enums import CommitStatus
from dangling_commits.domain.git_objects import Commit
from dangling_commits.domain.git_objects.branch import Branch
from dangling_commits.domain.utils import LocalObjectsHashes


class GitRepository():
    hostname: str
    folder: str
    repository: str

    def __init__(self, hostname: str, folder: str, repository: str) -> None:
        self.hostname = hostname
        self.folder = folder
        self.repository = repository

    @staticmethod
    def get_dangling_branches(dangling_commits_dict: dict[str, Commit],
                              local_commits: list[str]) -> list[Branch]:
        dangling_branches: list[Branch] = []

        dead = 0
        have_proper_parents: list[Commit] = []

        for commit in dangling_commits_dict.values():
            if commit.status not in (CommitStatus.FOUND, CommitStatus.ERASED):
                logging.debug(f"Unexpected status {commit}")
            elif commit.status == CommitStatus.ERASED:
                dead += 1
            elif commit.status == CommitStatus.FOUND:
                for parent in commit.parents:
                    if parent in local_commits:
                        if commit not in have_proper_parents:
                            have_proper_parents.append(commit)

                if not commit.children:
                    tree_length = 1
                    already_checked: set[str] = set()
                    to_check: set[str] = set()
                    origins: list[Commit] = []

                    for parent in commit.parents:
                        if parent in dangling_commits_dict.keys():
                            to_check.add(parent)
                        elif parent in local_commits:
                            origins.append(commit)
                        else:
                            logging.debug(f"unexpected {parent} for {commit}")

                    while to_check:
                        for parent in to_check.copy():
                            if parent not in already_checked:
                                tree_length += 1
                                if dangling_commits_dict[parent].parents:
                                    for grandfather in dangling_commits_dict[parent].parents:
                                        if grandfather in dangling_commits_dict.keys():
                                            to_check.add(grandfather)
                                        else:
                                            origins.append(dangling_commits_dict[parent])
                                else:
                                    logging.debug(
                                        f"No parents for {dangling_commits_dict[parent]}")

                            already_checked.add(parent)

                            to_check.remove((parent))
                    dangling_branches.append(
                        Branch(
                            end=commit,
                            origins=origins,
                            length=tree_length))
            else:
                logging.debug(f"unexpected status for {commit}")

        logging.info(f'Total erased dangling commits: {dead}')
        logging.info(f'Total recoverable dangling commits: {len(dangling_commits_dict) - dead}')
        logging.info(
            f'Total dangling commits with non-dangling parent: {len(have_proper_parents)}')
        logging.info(f'Total dangling trees: {len(dangling_branches)}')
        return dangling_branches

    def get_dangling_objects(
            self, localObjectHashes: LocalObjectsHashes) -> list[str]:
        raise NotImplementedError
