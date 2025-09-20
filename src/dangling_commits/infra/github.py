import json
import logging
import random
import shlex
import subprocess as sp
import time
from typing import Any

from dangling_commits.domain.exceptions import RepositoryError
from dangling_commits.domain.interfaces import GitRepository
from dangling_commits.domain.utils import LocalObjectsHashes


class Github(GitRepository):
    def __sleep_to_reset_rate_limit(self, rate_limit_info: dict[str, Any]) -> None:
        if rate_limit_info['remaining'] == 0:
            time_to_sleep = int(rate_limit_info['reset']) - time.time() + 10
            logging.debug(f"Primary rate limit encounteered, will sleep {int(time_to_sleep/60)}m")
            time.sleep(time_to_sleep)
        else:
            logging.debug("Probably secondary rate limit occured, will sleep 60s")
            time.sleep(60)

    def __query_api_binary(self, cmd: str) -> bytes:
        failed = 0

        while True:
            if failed > 2:
                raise RepositoryError("Maximum attempts to perform query reached")

            with sp.Popen(shlex.split(
                    cmd), stdout=sp.PIPE, stderr=sp.PIPE) as p:
                stdout, stderr = p.communicate()

            if p.returncode:
                failed += 1
                error_msg = stderr.decode().lower()

                if "rate limit" in error_msg:
                    with sp.Popen(shlex.split("gh api /rate_limit"), stdout=sp.PIPE, stderr=sp.PIPE) as p:
                        stdout, stderr = p.communicate()
                    rates = json.loads(stdout)

                    if cmd.startswith("gh api graphql"):
                        self.__sleep_to_reset_rate_limit(rates['resources']['graphql'])
                    elif cmd.startswith("gh api"):
                        self.__sleep_to_reset_rate_limit(rates['resources']['core'])
                    else:
                        raise Exception(f"Unknown command type {cmd}")

                elif any(msg in error_msg for msg in ["unexpected end of json input", "unexpected eof", "something went wrong while executing your query"]):
                    logging.debug('Request to the API failed while processing the response')
                    time.sleep(random.randint(1, 3))
                elif "please run:  gh auth login" in error_msg:
                    logging.error(error_msg)
                    raise RepositoryError(
                        "You need to authenticate with 'gh auth login' to run on a github server")
                else:
                    logging.warning(f"Unknown error occured: {error_msg} with {cmd}")
            else:
                break

        return stdout

    def __query_api(self, cmd: str) -> str:
        return self.__query_api_binary(cmd).decode()

    def __big_graphql_query(self, objects_sha: list[str], fragment: str,
                            jq_filter: str = '.data.repository',
                            retry_on_none_object: bool = False) -> dict[str, Any]:
        stdout: dict[str, Any] = {}

        query = f'{{repository(name:"{self.repository}", owner:"{self.folder}"){{'
        for sha in objects_sha:
            query = f'{query} sha_{sha}: object(oid:"{sha}"){{...infos}}'
        query = f'{query}}}}}\n{fragment}'

        try:
            stdout = json.loads(
                self.__query_api(f"gh api graphql -q '{jq_filter}' -f query='{query}'"))
            if retry_on_none_object:
                if None in stdout.values():
                    raise RepositoryError("Invalid answer")
        except RepositoryError as e:
            if str(e) == "Maximum attempts to perform query reached":
                logging.warning("Query failed too many times while getting the answser")
            elif str(e) == "Invalid answer":
                logging.warning(
                    "Query returned None answer for a specific object while it supposed to be impossible")
            else:
                raise

            logging.info(
                f"Since the initial query of {len(objects_sha)} objects is not working. Will split it and perform two individual query to overcome")
            half = len(objects_sha) // 2
            if half == 0:
                raise

            for objects in [objects_sha[:half], objects_sha[half:]]:
                stdout.update(
                    self.__big_graphql_query(objects,
                                             fragment,
                                             retry_on_none_object=retry_on_none_object))

        return stdout

    def __get_dangling_commits_hashes(self, local_commits: list[str]) -> set[str]:
        # get all possible commits hashes that can be found in /activity
        activity_commits = set(
            self.__query_api(
                f"gh api --paginate 'repos/{self.folder}/{self.repository}/activity' -q='.[] | .before, .after'").rstrip('\n').split('\n')
        )
        # no parent commit hash
        activity_commits.discard("0000000000000000000000000000000000000000")
        # can happen if activity is empty
        activity_commits.discard("")

        logging.info(f'Activity commits found: {len(activity_commits)}')

        # filter duplicate of local commits
        dangling_activity_commits = activity_commits.difference(local_commits)
        logging.info(f'Dangling activity commits found: {len(dangling_activity_commits)}')

        pull_request_commits = set(
            self.__query_api(
                f"gh api --paginate 'repos/{self.folder}/{self.repository}/pulls?state=all' -q='.[] | .base.sha, .head.sha, .merge_commit'").rstrip('\n').split('\n')
        )
        # no parent commit hash
        pull_request_commits.discard("0000000000000000000000000000000000000000")
        # can happen if no merge_commit exists
        pull_request_commits.discard("")

        logging.info(f'PRs commit found: {len(pull_request_commits)}')

        dangling_pr_commits = pull_request_commits.difference(local_commits)
        logging.info(f'Dangling PRs commits found: {len(dangling_pr_commits)}')

        logging.debug(
            f'{len(dangling_activity_commits - dangling_pr_commits)=}')
        logging.debug(
            f'{len(dangling_pr_commits- dangling_activity_commits )=}')
        logging.debug(f'{len(dangling_pr_commits & dangling_activity_commits)=}')
        logging.debug(f'{len(dangling_pr_commits | dangling_activity_commits)=}')

        return dangling_pr_commits | dangling_activity_commits

    def get_dangling_objects(
            self, localObjectHashes: LocalObjectsHashes) -> list[str]:
        dangling_commits_sha = self.__get_dangling_commits_hashes(localObjectHashes.commits)

        return list(dangling_commits_sha)
