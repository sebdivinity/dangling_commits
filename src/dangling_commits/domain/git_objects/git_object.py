from dataclasses import dataclass


@dataclass
class GitObject:
    sha: str

    def __hash__(self):
        return int(f'0x{self.sha}', base=16)
