from dataclasses import dataclass

from .commit import Commit


@dataclass
class Branch:
    end: Commit
    origins: list[Commit]
    length: int
