from dataclasses import dataclass

from dangling_commits.domain.exceptions import InvalidShaError
from dangling_commits.domain.utils import calculate_git_sha

from .git_object import GitObject


@dataclass
class TreeEntry(GitObject):
    mode: int
    name: str
    type: str


@dataclass
class Tree(GitObject):
    entries: list[TreeEntry]

    def git_file(self) -> bytearray:
        # tree binary format: [content size]\0[Entries having references to other trees and blobs]
        # entry binary format: [mode] [file/folder name]\0[SHA-1 of referencing blob or tree]
        # if you plan to pass this object to git hash-object -t tree --stdin be sure to omit the tree
        # 192\0 header, as otherwise you'll get fatal: corrupt tree file
        # https://stackoverflow.com/a/21599232
        tree = bytearray()

        for entry in self.entries:
            tree.extend(f'{entry.mode} {entry.name}\x00'.encode())
            tree.extend(bytearray.fromhex(entry.sha))

        calculated_sha = calculate_git_sha(bytes(tree), "tree")

        if calculated_sha != self.sha:
            raise InvalidShaError(
                f"Tree {self.sha} generated sha {calculated_sha}: {tree}")

        return tree

    def calculate_git_sha(self) -> str:
        tree = bytearray()

        for entry in self.entries:
            tree.extend(f'{entry.mode} {entry.name}\x00'.encode())
            tree.extend(bytearray.fromhex(entry.sha))

        return calculate_git_sha(bytes(tree), "tree")
