import json
import logging
import random
import shlex
import subprocess as sp
import time
from typing import Any

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


class Github(GitRepository):
    def __get_dangling_commits(self, dangling_commits_sha: set[str],
                               local_commits: list[str]) -> list[Commit]:

        dangling_commits: dict[str, Commit] = {
            sha: Commit(sha=sha, status=CommitStatus.UNKNOWN, parents=set(), children=set())
            for sha in dangling_commits_sha}

        iteration = 0
        window_size = 200
        fragment = """fragment infos on Commit {
    history(first: 10) {
        totalCount
        pageInfo {
        endCursor
        hasNextPage
        }
        nodes {
        oid
        tree {
            oid
        }
        signature {
            signature
            state
            payload
        }
        parents(first: 10) {
            totalCount
            nodes {
            oid
            }
        }
        message
        committer {
            date
            email
            name
        }
        author {
            date
            email
            name
        }
        }
    }
    }"""

        logging.info("Retrieving dangling commits content")
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

            stdout = self.__big_graphql_query([c.sha for c in to_query[:window_size]], fragment)

            for commit_sha, fields in stdout.items():
                commit_sha = commit_sha.removeprefix("sha_")
                logging.debug(f'= Checking {commit_sha} history =')

                if fields is None or fields == {}:
                    logging.debug(f'{commit_sha.removeprefix("sha_")} is erased')
                    dangling_commits[commit_sha].status = CommitStatus.ERASED
                    continue

                for commit_info in fields["history"]["nodes"]:
                    sha: str = commit_info["oid"]

                    if sha in local_commits:
                        logging.debug(f'Ignoring local commit {sha}')
                        continue

                    logging.debug(f'Checking {sha} commit')

                    if len(commit_info["parents"]["nodes"]) != int(
                            commit_info["parents"]["totalCount"]):
                        raise Exception(f"Unhandled non-retrieved parent: {commit_info}")

                    if dangling_commits[sha].status == CommitStatus.FOUND:
                        logging.debug(f'{sha} was already found')
                    else:
                        logging.debug(f'Switching {dangling_commits[sha].status} to FOUND')
                        dangling_commits[sha].status = CommitStatus.FOUND

                    dangling_commits[sha].status = CommitStatus.FOUND
                    dangling_commits[sha].tree = commit_info["tree"]["oid"]
                    dangling_commits[sha].message = commit_info["message"]
                    dangling_commits[sha].author = AuthorOrCommitter(
                        date=commit_info["author"]["date"],
                        email=commit_info["author"]["email"],
                        name=commit_info["author"]["name"])
                    dangling_commits[sha].committer = AuthorOrCommitter(
                        date=commit_info["committer"]["date"],
                        email=commit_info["committer"]["email"],
                        name=commit_info["committer"]["name"])

                    if commit_info["signature"] is None:
                        dangling_commits[sha].signature = CommitSignature(
                            status=CommitSignatureStatus.UNSIGNED)
                    else:
                        try:
                            status = CommitSignatureStatus[commit_info["signature"]["state"]]
                        except KeyError as exc:
                            raise RepositoryError(
                                f"Unhandled signature status: {commit_info}") from exc

                        dangling_commits[sha].signature = CommitSignature(
                            status=status,
                            payload=commit_info["signature"]["payload"],
                            signature=commit_info["signature"]["signature"]
                        )

                    for parent in commit_info["parents"]["nodes"]:
                        dangling_commits[sha].parents.add(parent['oid'])

                        if parent['oid'] in dangling_commits.keys():
                            dangling_commits[parent["oid"]].children.add(sha)

                            if dangling_commits[parent["oid"]].status == CommitStatus.UNKNOWN:
                                logging.debug(
                                    f'switch parent commit {dangling_commits[parent["oid"]].sha} to INCOMPLETE')
                                dangling_commits[parent["oid"]].status = CommitStatus.INCOMPLETE
                            elif dangling_commits[parent["oid"]].status not in (CommitStatus.FOUND, CommitStatus.INCOMPLETE):
                                raise RepositoryError(
                                    f'Unexpected status {dangling_commits[parent["oid"]]}')

                        elif parent['oid'] not in local_commits:
                            logging.debug(f'new parent dangling commit found {parent["oid"]}')
                            dangling_commits[parent["oid"]] = Commit(
                                sha=parent["oid"], status=CommitStatus.INCOMPLETE, parents=set(), children={sha, })

            iteration += 1

        return list(dangling_commits.values())

    def __get_dangling_trees_and_blobs(self, dangling_commits: list[Commit],
                                       local_trees: list[str],
                                       local_blobs: list[str]) -> tuple[list[Tree],
                                                                        list[Blob]]:
        trees: dict[str, Tree] = {}
        blobs: dict[str, Blob] = {}

        iteration = 0
        window_size = 500

        fragment = """fragment infos on Tree {
        oid
        entries{
            mode
            name
            oid
            type
        }
    }"""

        to_query: set[str] = {c.tree for c in filter(
            lambda c: c not in local_trees,
            (c for c in dangling_commits if c.status == CommitStatus.FOUND))}

        logging.info("Retrieving dangling trees content")
        while True:
            logging.info(f'== Iteration #{iteration} ==')

            logging.info(f'{len(trees)}/{len(to_query)+len(trees)}')

            if not to_query:
                logging.info("Nothing to do in this iteration")
                break

            stdout = self.__big_graphql_query(
                list(to_query)[: window_size],
                fragment, retry_on_none_object=True)

            for key, tree_info in stdout.items():
                if tree_info is None:
                    raise CommandExecutionError(
                        f"Impossible to get None here normally for {key.removeprefix('sha_')}")
                logging.debug(f'= Checking {tree_info["oid"]} entries =')

                tree = Tree(sha=tree_info["oid"], entries=[])

                for entry in tree_info["entries"]:
                    # if entry["oid"] not in trees and entry["oid"] not in blobs and
                    # entry["oid"] not in local_trees vand entry["oid"] not in local_blobs:
                    if all(entry["oid"] not in l for l in [
                            trees, blobs, local_trees, local_blobs]):
                        if entry["type"] == "tree":
                            logging.debug(f'Found new dangling tree {entry["oid"]}')
                            to_query.add(entry["oid"])
                        elif entry["type"] == "blob":
                            logging.debug(f'Found new dangling blob {entry["oid"]}')
                            blobs[entry["oid"]] = Blob(entry["oid"])
                        elif entry["type"] == "commit":
                            # submodule here, nothing to do
                            pass
                        else:
                            raise Exception(
                                f"Unexpected entry type {entry} for {tree_info['oid']}")

                    # tree
                    if entry["mode"] == 16384:
                        mode = 40000
                    # normal file
                    elif entry["mode"] == 33188:
                        mode = 100644
                    # executable
                    elif entry["mode"] == 33261:
                        mode = 100755
                    # symlink
                    elif entry["mode"] == 40960:
                        mode = 120000
                    # submodule
                    elif entry["mode"] == 57344:
                        mode = 160000
                    else:
                        raise Exception(f"unknown mode: {entry['mode']}")

                    tree.entries.append(
                        TreeEntry(
                            entry["oid"],
                            mode,
                            entry["name"],
                            entry["type"]))

                trees[tree_info["oid"]] = tree
                to_query.remove(tree.sha)

            iteration += 1

        logging.info(f'Tree unknown: {len(trees)}')
        logging.info(f'Blob unknown: {len(blobs)}')
        return list(trees.values()), list(blobs.values())

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
            self, localObjectHashes: LocalObjectsHashes) -> tuple[list[Commit], list[Blob], list[Tree], list[Branch]]:
        dangling_commits_sha = self.__get_dangling_commits_hashes(localObjectHashes.commits)
        dangling_commits = self.__get_dangling_commits(
            dangling_commits_sha, localObjectHashes.commits)

        dangling_trees, dangling_blobs = self.__get_dangling_trees_and_blobs(
            dangling_commits, localObjectHashes.trees, localObjectHashes.blobs)

        dangling_branches = self.get_dangling_branches(dangling_commits, localObjectHashes.commits)

        return dangling_commits, dangling_blobs, dangling_trees, dangling_branches

    def create_blobs(self, blobs: list[Blob]):
        to_dl = [blob.sha for blob in blobs]
        rest_dl: list[str] = []
        graphql_dl: list[str] = []

        logging.info("Classyfing blobs to optimize retrieving")
        idx = 0
        windows_size = 1000
        iteration = 0
        fragment = """fragment infos on Blob {
        oid
        byteSize
        isBinary
    }"""

        while True:
            logging.info(f'== Iteration #{iteration} ==')
            logging.info(f'{idx}/{len(blobs)}')

            to_query = to_dl[idx:windows_size + idx]

            if not to_query:
                logging.info("Nothing to do in this iteration")
                break

            stdout = self.__big_graphql_query(
                to_query[: windows_size],
                fragment, retry_on_none_object=True)

            for blob_info in stdout.values():
                if blob_info["isBinary"]:
                    rest_dl.append(blob_info["oid"])
                    logging.debug(
                        f'Blob {blob_info["oid"]} is binary, will DL with rest API afterwards')
                elif blob_info["byteSize"] > 512000:
                    rest_dl.append(blob_info["oid"])
                    logging.debug(
                        f'Blob {blob_info["oid"]} is too huge for graphql, while DL with rest API afterwards')
                else:
                    graphql_dl.append(blob_info["oid"])

            idx += len(to_query)
            iteration += 1

        logging.info(f"Found {len(graphql_dl)} blobs to DL with graphql API")
        idx = 0
        windows_size = 200
        iteration = 0
        fragment = """fragment infos on Blob {
        text
        oid
        byteSize
        isBinary
    }"""

        while True:
            logging.info(f'== Iteration #{iteration} ==')
            logging.info(f'{idx}/{len(graphql_dl)}')

            to_query = graphql_dl[idx:windows_size + idx]

            if not to_query:
                logging.info("Nothing to do in this iteration")
                break

            stdout = self.__big_graphql_query(
                to_query[: windows_size],
                fragment, retry_on_none_object=True)

            for key, blob_info in stdout.items():
                if blob_info is None:
                    raise RepositoryError(
                        f"Impossible to get None here normally for {key.removeprefix('sha_')}")

                calculated_sha = calculate_git_sha(
                    data=blob_info["text"].encode(), object_type="blob")

                if calculated_sha != blob_info["oid"]:
                    logging.debug(
                        f"Blob {blob_info['oid']} generated invalid sha ({calculated_sha}), will DL with REST")
                    rest_dl.append(blob_info['oid'])
                else:
                    create_object(blob_info["text"].encode(), "blob", blob_info["oid"])
                    logging.debug(f'Blob {blob_info["oid"]} created')

            idx += len(to_query)
            iteration += 1

        logging.info(f"Found {len(rest_dl)} blobs to DL with rest API")
        for idx, blob_sha in enumerate(rest_dl):
            encoded_blob = self.__query_api_binary(
                f"gh api 'repos/{self.folder}/{self.repository}/git/blobs/{blob_sha}' -q '.content'")
            decoded_blob = exec_cmd_binary("base64 -d", stdin=encoded_blob)
            calculated_sha = calculate_git_sha(data=decoded_blob, object_type="blob")

            if calculated_sha != blob_sha:
                raise InvalidShaError(
                    f"Blob created does not have the same sha as the original: {calculated_sha=} != {blob_sha=}")
            else:
                create_object(decoded_blob, "blob", blob_sha)
                logging.debug(
                    'Binary (or huge) blob %s created (%d/%d)',
                    blob_sha,
                    idx + 1,
                    len(rest_dl))
