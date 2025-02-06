import json
import logging
import os
import random
import shlex
import subprocess as sp
import time
from collections import defaultdict
from functools import cache
from pprint import pp
from typing import Any, Union

import requests

from dangling_commits.domain.enums import CommitSignatureStatus, CommitStatus
from dangling_commits.domain.exceptions import (CommandExecutionError,
                                                InvalidShaError,
                                                RepositoryError)
from dangling_commits.domain.git_objects import (AuthorOrCommitter, Blob,
                                                 Branch, Commit,
                                                 CommitSignature, Tree,
                                                 TreeEntry)
from dangling_commits.domain.interfaces import GitRepository
from dangling_commits.domain.utils import (LocalObjectsHashes,
                                           calculate_git_sha, create_object,
                                           exec_cmd_binary)


class Gitlab(GitRepository):
    def __init__(self, hostname: str, folder: str, repository: str) -> None:
        super().__init__(hostname, folder, repository)
        self.project: str = f'{self.folder}/{self.repository}'.replace("/", "%2f")
        self.session = requests.Session()
        self.session.headers.update({'PRIVATE-TOKEN': os.environ['GITLAB_TOKEN']})

    def __get_dangling_commits(self, dangling_commits_sha: set[str],
                               local_commits: list[str]) -> list[Commit]:
        def handle_new_commit_info(c_info: dict[str, Any]):
            nonlocal dangling_commits
            sha = c_info["id"]

            if dangling_commits[sha].status == CommitStatus.FOUND:
                raise RepositoryError(f'{c_info} was already found (should not be possible)')

            logging.debug(f'Switching commit {sha} {dangling_commits[sha].status.name} to FOUND')
            dangling_commits[sha].status = CommitStatus.FOUND
            dangling_commits[sha].message = c_info["message"]
            dangling_commits[sha].author = AuthorOrCommitter(
                date=c_info["authored_date"],
                email=c_info["author_email"],
                name=c_info["author_name"])
            dangling_commits[sha].committer = AuthorOrCommitter(
                date=c_info["committed_date"],
                email=c_info["committer_email"],
                name=c_info["committer_name"])

            for parent in c_info["parent_ids"]:
                dangling_commits[sha].parents.add(parent)

                if parent in dangling_commits.keys():
                    dangling_commits[parent].children.add(sha)

                    if dangling_commits[parent].status == CommitStatus.UNKNOWN:
                        logging.debug(
                            f'switch parent commit {dangling_commits[parent].sha} to INCOMPLETE')
                        dangling_commits[parent].status = CommitStatus.INCOMPLETE
                    elif dangling_commits[parent].status not in (CommitStatus.FOUND, CommitStatus.INCOMPLETE):
                        raise RepositoryError(
                            f'Unexpected status {dangling_commits[parent]}')

                elif parent not in local_commits:
                    logging.debug(f'new parent dangling commit found {parent}')
                    dangling_commits[parent] = Commit(
                        sha=parent, status=CommitStatus.INCOMPLETE, parents=set(),
                        children={sha, })

        dangling_commits: dict[str, Commit] = {
            sha: Commit(sha=sha, status=CommitStatus.UNKNOWN, parents=set(), children=set())
            for sha in dangling_commits_sha}

        logging.info("Retrieving dangling commits content")
        iteration = 0

        logging.debug("Getting as much as we can from /repository/commits")
        commits = json.loads(self.__query_api(
            f'api/v4/projects/{self.project}/repository/commits?all=true', paginate=True))
        for c in commits:
            if c["id"] in dangling_commits:
                handle_new_commit_info(c)
        logging.info(
            f'Found {len([c for c in dangling_commits.values() if c.status == CommitStatus.FOUND])} commits info with /repository/commits')

        while True:
            logging.info(f'== Iteration #{iteration} ==')

            to_query = list(filter(
                lambda c: c.status in (
                    CommitStatus.INCOMPLETE,
                    CommitStatus.UNKNOWN),
                dangling_commits.values()))

            logging.info(f'{len(dangling_commits)-len(to_query)}/{len(dangling_commits)}')

            if not to_query:
                logging.info("Nothing to do in this iteration")
                break

            for c in to_query:
                logging.debug(f'Checking {c.sha} commit')

                try:
                    commit_info = json.loads(
                        self.__query_api(f"api/v4/projects/{self.project}/repository/commits/{c.sha}"))
                except RepositoryError as e:
                    if str(e).startswith("Unexpected 404 on request"):
                        dangling_commits[c.sha].status = CommitStatus.ERASED
                else:
                    handle_new_commit_info(commit_info)

            iteration += 1

        return list(dangling_commits.values())

    def __get_dangling_commits_graphql(self, dangling_commits_sha: set[str],
                                       local_commits: list[str]) -> list[Commit]:
        dangling_commits: dict[str, Commit] = {
            sha: Commit(sha=sha, status=CommitStatus.UNKNOWN, parents=set(), children=set())
            for sha in dangling_commits_sha}

        logging.info("Retrieving dangling commits content")
        iteration = 0
        window_size = 200
        fragment = "fragment infos on Tree{lastCommit{sha authorEmail authorName authoredDate committedDate committerEmail committerName message signature{verificationStatus}}}"

        while True:
            logging.info(f'== Iteration #{iteration} ==')

            to_query = list(filter(
                lambda c: c.status in (
                    CommitStatus.INCOMPLETE,
                    CommitStatus.UNKNOWN),
                dangling_commits.values()))

            logging.info(f'{len(dangling_commits)-len(to_query)}/{len(dangling_commits)}')

            if not to_query:
                logging.info("Nothing to do in this iteration")
                break

            commits_info = self.__big_graphql_query(
                [c.sha for c in to_query[:window_size]], fragment)
            print(f'{commits_info=}')

        return list(dangling_commits.values())

    def __get_dangling_trees_and_blobs(self, dangling_commits: list[Commit],
                                       localObjectsHashes: LocalObjectsHashes) -> tuple[list[Commit], list[Tree],
                                                                                        list[Blob]]:
        trees: dict[str, Tree] = {}
        blobs: dict[str, Blob] = {}

        for c in (c for c in dangling_commits if c.status == CommitStatus.FOUND):
            # is root Tree of current dangling commit
            c_key = f".{c.sha}"
            tree_info = json.loads(
                self.__query_api(
                    f'api/v4/projects/{self.project}/repository/tree?ref={c.sha}&recursive=true',
                    paginate=True))

            tree_dict: dict[str, list[TreeEntry]] = defaultdict(list)
            for t in tree_info:
                folder = '/'.join(t["path"].split("/")[:-1]) if "/" in t["path"] else c_key
                entry = TreeEntry(
                    sha=t["id"],
                    mode=int(t["mode"]),
                    type=t["type"],
                    name=t["name"])

                # if entry.sha not in tree_dict[folder]:
                # two tree entry can have the same sha but not the same name
                # the name is used to get back
                tree_dict[folder].append(entry)

                # new dangling blob found
                if entry.type == "blob" and all([entry.sha not in keys
                                                for keys in [blobs, localObjectsHashes.blobs]]):
                    blobs[entry.sha] = Blob(entry.sha)

            for folder in tree_dict:
                parent_folder = '/'.join(folder.split("/")[:-1]) if "/" in folder else c_key
                for entry in tree_dict[parent_folder]:
                    folder_name = folder.split("/")[-1] if "/" in folder else folder
                    if folder_name == entry.name or folder == c_key:
                        # if subfolder sha is found in entry
                        sha = entry.sha
                        # if root folder, we need to calculate sha
                        if folder == c_key:
                            logging.debug(f"Updating tree of commit {c.sha}")
                            sha = Tree(sha="0", entries=tree_dict[folder]).calculate_git_sha()
                            c.tree = sha

                        # new dangling tree found
                        if all([sha not in keys for keys in [
                               trees, localObjectsHashes.trees]]):
                            logging.debug(f"Adding tree {folder} sha: {sha}")
                            trees[sha] = Tree(sha, entries=tree_dict[folder])
                        break
                else:
                    raise RepositoryError(f"entry of folder {folder} not found in tree_dict")

        return dangling_commits, list(trees.values()), list(blobs.values())

    def __big_graphql_query(
            self, objects_sha: list[str], fragment: str, paginate: bool = False) -> dict[str, Any]:
        stdout: dict[str, Any] = {}

        query = f'{{project(fullPath:"{self.folder}/{self.repository}"){{repository{{'
        for sha in objects_sha:
            query = f'{query} sha_{sha}: tree(ref:"{sha}"){{...infos}}'
        query = f'{query}}}}}}}\n{fragment}'
        print(query)
        try:
            stdout = self.__query_graphql(query)
        except RepositoryError:
            logging.info(
                f"Since the initial query of {len(objects_sha)} objects is not working. Will split it and perform two individual query to overcome")
            half = len(objects_sha) // 2
            if half == 0:
                raise

            for objects in [objects_sha[:half], objects_sha[half:]]:
                stdout.update(
                    self.__big_graphql_query(objects, fragment))

        return stdout

    def __query_graphql(self, query: str) -> dict[str, Any]:
        post_data = {"query": query, "variables": None}
        r = self.session.post(f"https://{self.hostname}/api/graphql", data=post_data)

        if r.status_code != 200:
            logging.error(r.text)
            raise RepositoryError(f"Unexpected {r.status_code} on graphql with query {query}")

        return r.json()

    @cache
    def __query_api(self, path: str, paginate: bool = False,
                    binary: bool = False) -> Union[str, bytes]:
        # handle rate limit
        # should not apply to authenticated user /projects
        # should not apply to all requests for /repository
        # https://docs.gitlab.com/ee/administration/settings/rate_limit_on_projects_api.html
        # https://docs.gitlab.com/ee/administration/settings/files_api_rate_limits.html
        if paginate:
            if '?' in path:
                url = f'https://{self.hostname}/{path}&per_page=100'
            else:
                url = f'https://{self.hostname}/{path}?per_page=100'
        else:
            url = f'https://{self.hostname}/{path}'

        page = 1
        response = ""
        json_response: list[Any] = []

        while True:
            if paginate:
                r = self.session.get(f'{url}&page={page}')
            else:
                r = self.session.get(url)

            if r.status_code != 200:
                logging.error(r.text)
                raise RepositoryError(f"Unexpected {r.status_code} on request {url}")

            if not paginate:
                response = r.text if not binary else r.content
                break

            else:
                json_response += r.json()

                if r.headers["x-next-page"] == "":
                    response = json.dumps(json_response)
                    break
                page += 1

        return response

    def __get_dangling_commits_hashes(self, localObjectHashes: LocalObjectsHashes) -> set[str]:
        # GitLab removes events older than 3 years from the events table for performance reasons.
        # vérifier si /activity dans le UI n'a pas la limite de 3 ans, mais ça ne
        # ferait pas de sens à priori
        events = json.loads(self.__query_api(
            f'api/v4/projects/{self.project}/events?action=pushed', paginate=True))
        events_commits: set[str] = set()

        for event in events:
            if 'push_data' in event:
                if event['push_data']['commit_from'] is not None:
                    events_commits.add(event['push_data']['commit_from'])
                if event['push_data']['commit_to'] is not None:
                    events_commits.add(event['push_data']['commit_to'])

        logging.info(f'Event commits found: {len(events_commits)}')
        # filter duplicate of local commits
        dangling_event_commits = events_commits.difference(
            localObjectHashes.commits).difference(
            localObjectHashes.tags)
        logging.info(f'Dangling event commits found: {len(dangling_event_commits)}')

        merge_requests = json.loads(self.__query_api(
            f'api/v4/projects/{self.project}/merge_requests?state=all', paginate=True))
        merge_commits: set[str] = set()
        for mr in merge_requests:
            if mr['sha'] is not None:
                merge_commits.add(mr['sha'])
            if mr['merge_commit_sha'] is not None:
                merge_commits.add(mr['merge_commit_sha'])
            if mr['squash_commit_sha'] is not None:
                merge_commits.add(mr['squash_commit_sha'])

        logging.info(f'MRs commit found: {len(merge_commits)}')
        dangling_mr_commits = merge_commits.difference(
            localObjectHashes.commits).difference(
            localObjectHashes.tags)
        logging.info(f'Dangling MRs commits found: {len(dangling_mr_commits)}')

        commits = json.loads(self.__query_api(
            f'api/v4/projects/{self.project}/repository/commits?all=true', paginate=True))
        commits_commits: set[str] = set()
        for commit in commits:
            commits_commits.add(commit["id"])
            for parent_commit in commit["parent_ids"]:
                commits_commits.add(parent_commit)

        logging.info(f'/repository/commits commit found: {len(commits_commits)}')
        dangling_commits_commits = commits_commits.difference(
            localObjectHashes.commits).difference(
            localObjectHashes.tags)
        logging.info(
            f'Dangling /repository/commits commits found: {len(dangling_commits_commits)}')

        dangling_commits = dangling_mr_commits | dangling_event_commits | dangling_commits_commits

        logging.debug(
            f'Dangling commits present uniquely from merge_request: {len(dangling_mr_commits - dangling_event_commits - dangling_commits_commits)}')
        logging.debug(
            f'Dangling commits present uniquely from events: {len(dangling_event_commits - dangling_mr_commits - dangling_commits_commits)}')
        logging.debug(
            f'Dangling commits present uniquely from /repository/commits: {len(dangling_commits_commits - dangling_mr_commits - dangling_event_commits )}')
        logging.debug(f'Total dangling commits found: {len(dangling_commits)}')

        return dangling_commits

    def __get_dangling_objects(
            self, dangling_commits_sha: set[str], localObjectHashes: LocalObjectsHashes) -> tuple[list[Commit], list[Tree], list[Blob]]:
        dangling_commits = self.__get_dangling_commits(
            set(list(dangling_commits_sha)[:30]), localObjectHashes.commits)
        # TODO: see if I can use graphql to retrieve arborescence in parrallel
        return self.__get_dangling_trees_and_blobs(
            dangling_commits, localObjectHashes)

    def get_dangling_objects(
            self, localObjectHashes: LocalObjectsHashes) -> tuple[list[Commit], list[Blob], list[Tree], list[Branch]]:
        dangling_commits_sha = self.__get_dangling_commits_hashes(localObjectHashes)

        dangling_commits, dangling_trees, dangling_blobs = self.__get_dangling_objects(
            dangling_commits_sha, localObjectHashes)

        dangling_branches = self.get_dangling_branches(dangling_commits, localObjectHashes.commits)

        return dangling_commits, dangling_blobs, dangling_trees, dangling_branches

    def create_blobs(self, blobs: list[Blob]):
        # use graphql API to get multiple blobs at once
        # but we need the commit hash and the path inside the tree
        # seems like we can't directly request the blob with its hash
        #         {
        # project(fullPath: "ettic/tools/bifrost") {
        #     repository {
        #     blobs(first:100, ref:"main", paths:"README.md"){
        #         nodes
        #         {rawTextBlob}
        #     }
        #     }
        # }
        # }

        for blob in blobs:
            data = self.__query_api(
                f'api/v4/projects/{self.project}/repository/blobs/{blob.sha}/raw', binary=True)

            if calculate_git_sha(data=data, object_type="blob") != blob.sha:
                raise RepositoryError(f"Invalid sha obtained after downloading blob {blob.sha}")

            create_object(data, "blob", blob.sha)
