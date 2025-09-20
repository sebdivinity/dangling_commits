import json
import logging
import os
from functools import cache
from typing import Any, Union

import requests

from dangling_commits.domain.exceptions import RepositoryError
from dangling_commits.domain.interfaces import GitRepository
from dangling_commits.domain.utils import LocalObjectsHashes


class Gitlab(GitRepository):
    def __init__(self, hostname: str, folder: str, repository: str) -> None:
        super().__init__(hostname, folder, repository)
        self.project: str = f'{self.folder}/{self.repository}'.replace("/", "%2f")
        self.session = requests.Session()
        self.session.headers.update({'PRIVATE-TOKEN': os.environ['GITLAB_TOKEN']})

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

    def get_dangling_objects(
            self, localObjectHashes: LocalObjectsHashes) -> list[str]:
        dangling_commits_sha = self.__get_dangling_commits_hashes(localObjectHashes)

        return list(dangling_commits_sha)
