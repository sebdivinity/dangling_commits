import datetime
import logging
import string
from dataclasses import dataclass
from itertools import permutations
from typing import Union

from dangling_commits.domain.enums import CommitSignatureStatus, CommitStatus
from dangling_commits.domain.exceptions import InvalidShaError
from dangling_commits.domain.utils import calculate_git_sha

from .git_object import GitObject


@dataclass
class AuthorOrCommitter:
    date: str
    email: str
    name: str

    def __str__(self) -> str:
        # try:
        date = datetime.datetime.fromisoformat(self.date)
        # except ValueError:
        #     date = datetime.datetime.strptime(self.date, "%Y-%m-%dT%H:%M:%SZ")
        #     return f'{self.name} <{self.email}> {int(date.timestamp())} +0000'
        # else:
        return f'{self.name} <{self.email}> {int(date.timestamp())} {date.strftime("%z")}'


@dataclass
class CommitSignature:
    status: CommitSignatureStatus
    signature: Union[str, None] = None
    payload: Union[str, None] = None


@dataclass
class Commit(GitObject):
    status: CommitStatus
    parents: set[str]
    children: set[str]
    tree: Union[str, None] = None
    author: Union[AuthorOrCommitter, None] = None
    committer: Union[AuthorOrCommitter, None] = None
    message: Union[str, None] = None
    signature: Union[CommitSignature, None] = None

    def git_file(self) -> str:
        commit = ''

        if self.status != CommitStatus.FOUND:
            raise Exception(f"Impossible to generate git file with not fully found commit: {self}")

        if self.signature is not None and self.signature.status != CommitSignatureStatus.UNSIGNED:
            # commit = self.__get_git_file_signed()

            # this prevent multiple inclusion if we found 'committer' in commit message
            # this happen for example with conflicts message
            signatured_added = False
            for line in self.signature.payload.split('\n'):
                if commit != '':
                    commit = f'{commit}\n{line}'
                else:
                    commit = f'{line}'
                if not signatured_added and line.startswith("committer"):
                    signatured_added = True
                    commit = f'{commit}\ngpgsig'
                    for sig_line in self.signature.signature.split('\n'):
                        commit = f'{commit} {sig_line}\n'
                    # remove last trailing line
                    commit = commit[:-1]

            calculated_sha = calculate_git_sha(commit.encode(), "commit")
            if calculated_sha != self.sha:
                logging.debug(self)
                logging.debug(commit)
                raise InvalidShaError(f"{calculated_sha=} != {self.sha}")

        else:
            messages = [self.message + '\n', self.message, self.message + '\n\n']
            # Control character from Start Of Heading to Unit Separator
            # Github will encode control characters as ^CHAR
            # For exemple Start Of Text \x02 will be encoded as ^B in github answers
            # So we need to replace back to the hex value to get the right sha
            for idx, char in enumerate(string.ascii_uppercase + "[\\]^_"):
                # generate \x01, \x02 but in a string that is not interpreted as a unicode
                # escape sequence, we can display it's value in a print
                replace_value_display = f'{idx+1:#04x}'.replace('0x', '\\x')
                # generate ^A ^B, etc...
                to_replace = f'^{char}'
                if to_replace in self.message:
                    logging.debug(f"converting {to_replace} to {replace_value_display}")
                    messages = [m.replace(to_replace, f'{chr(idx+1)}') for m in messages]
            parents = list(permutations(self.parents))

            authors = self.__generate_author_or_committer_str(self.author)
            committers = self.__generate_author_or_committer_str(self.committer)

            for idx, commit in enumerate((
                    self.get_git_file_unsigned(parents=p, author=a, committer=c, message=m)
                    for m in messages
                    for p in parents
                    for a in authors
                    for c in committers)):
                calculated_sha = calculate_git_sha(commit.encode(), object_type="commit")

                if calculated_sha == self.sha:
                    max = len(messages) * len(parents) * len(authors) * len(committers)
                    logging.debug(f"Got valid commit content after {idx+1} variations ({max=})")
                    return commit

            # raise Error if loop finished ended
            raise InvalidShaError(
                f"Commit {self.sha} generated sha {calculated_sha}: {commit}")
        return commit

    def __generate_author_or_committer_str(self, person: AuthorOrCommitter) -> list[str]:
        try:
            return [str(person)]
        except ValueError:
            date = datetime.datetime.strptime(person.date, "%Y-%m-%dT%H:%M:%SZ")
            strings: list[str] = []
            timestamp = int(date.timestamp())
            for i in range(1, 24):
                offset = i * 60 * 60
                strings.extend([
                    f'{person.name} <{person.email}> {timestamp-offset} +0000',
                    f'{person.name} <{person.email}> {timestamp+offset} +0000',
                    f'{person.name} <{person.email}> {timestamp-offset} -{i:02}00',
                    f'{person.name} <{person.email}> {timestamp+offset} +{i:02}00'])
            return strings

    def get_git_file_unsigned(
            self,
            parents: Union[list[str], None] = None,
            author: Union[str, None] = None,
            committer: Union[str, None] = None,
            message: Union[str, None] = None) -> str:
        if parents is None:
            parents = self.parents
        if author is None:
            author = str(self.author)
        if committer is None:
            committer = str(self.committer)
        if message is None:
            message = self.message

        commit = f'tree {self.tree}\n'

        for parent in parents:
            commit = f'{commit}parent {parent}\n'
        commit = f'{commit}author {author}\n'
        commit = f'{commit}committer {committer}\n\n'
        commit = f'{commit}{message}'

        return commit

    def __get_git_file_signed(self, parents: Union[list[str], None] = None) -> str:
        if parents is None:
            parents = self.parents

        commit = f'tree {self.tree}\n'

        for parent in parents:
            commit = f'{commit}parent {parent}\n'
        commit = f'{commit}author {self.author}\n'
        commit = f'{commit}committer {self.committer}'
        commit = f'{commit}\ngpgsig'
        for sig_line in self.signature.signature.split('\n'):
            commit = f'{commit} {sig_line}\n'
        commit = f'{commit}\n{self.message}'
        return commit
