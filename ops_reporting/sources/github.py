import json
import logging
import os.path
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor
from mimetypes import guess_type
from tempfile import TemporaryDirectory

import backoff
import requests

from ops_reporting.sources import Source, Attachment
from util.conf_util import SettingsConf

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)


# TODO: Should this be moved to a config/setting?
SDS_QUERY_WF_ID = 107393962


def _fatal_code(err: Exception) -> bool:
    if isinstance(err, requests.exceptions.RequestException) and err.response is not None:
        return err.response.status_code not in [401, 418, 429, 500, 502, 503, 504]
    return False


def _backoff_logger(details):
    logger.warning(
        f"Backing off {details['target']} function for {details['wait']:0.1f} "
        f"seconds after {details['tries']} tries."
    )
    logger.warning(f"Total time elapsed: {details['elapsed']:0.1f} seconds.")


@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=_fatal_code,
                      on_backoff=_backoff_logger,
                      interval=15)
@backoff.on_exception(backoff.expo,
                      (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
                      max_tries=2)
def _get(url, params=None, headers=None, response_json=True, **kwargs):
    if params is None:
        params = {}

    if headers is None:
        headers = {}

    response = requests.get(url, params=params, headers=headers, **kwargs)
    logger.info(f'GET {response.request.url} : [{response.status_code}]')
    try:
        response.raise_for_status()
    except:
        logger.error(f'{response.text}')
        raise

    return response.json() if response_json else response


class GithubSource(Source):
    """Source class for data from opera-sds Github Actions"""

    _settings = SettingsConf().cfg
    
    def __init__(self):
        super().__init__('SDS_Status', None)

        self._executor = None
        self._future = None
        self._tmp_dir = None

    def __enter__(self):
        self._tmp_dir = TemporaryDirectory()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='github_artifacts-')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmp_dir.cleanup()
        self._tmp_dir = None

        self._executor.shutdown(wait=True)
        self._executor = None

    def run(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir has not been created, please run in with block')

        self._future = self._executor.submit(self._get_sds_data)

    def _get_sds_data(self):
        try:
            if not self._settings.get('GIT_PUB_ACCESS_TOKEN'):
                raise RuntimeError('No Git token provided')

            artifact_url = self._get_latest_sds_artifact(token=self._settings.get('GIT_PUB_ACCESS_TOKEN'))

            artifact_zip_path = os.path.join(self._tmp_dir.name, 'artifact.zip')

            logger.info(f'Downloading SDS artifact from {artifact_url} to {artifact_zip_path}')

            dl_response = _get(
                artifact_url,
                headers={'Authorization': f'Bearer {self._settings.get("GIT_PUB_ACCESS_TOKEN")}'},
                response_json=False,
                stream=True
            )

            with open(artifact_zip_path, 'wb') as f:
                for chunk in dl_response.iter_content(chunk_size=1024 ** 2):
                    if chunk:
                        f.write(chunk)

            logger.info(f'Downloaded SDS artifact from {artifact_zip_path}')

            extract_dir = os.path.join(self._tmp_dir.name, 'artifacts')
            os.makedirs(extract_dir, exist_ok=True)

            with zipfile.ZipFile(artifact_zip_path) as zf:
                zf.extractall(extract_dir)

            os.unlink(artifact_zip_path)

            logger.info(f'Extracted {len(os.listdir(extract_dir))} files')

            return True, extract_dir, None
        except Exception as e:
            logger.error(e)
            return False, None, str(e)

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        success, artifacts_dir, error = self._future.result()

        if not success:
            self._data = {}
            self._errors.append(
                f'SDS status retrieval failed: {error}'
            )
            return

        files = os.listdir(artifacts_dir)

        if 'query_results.json' not in files:
            self._data = {}
            self._errors.append(
                'SDS Query metadata file not in returned artifacts'
            )
            return

        with open(os.path.join(artifacts_dir, 'query_results.json')) as f:
            query_results = json.load(f)

        for product in query_results.get('products', {}).keys():
            if 'counts' in query_results['products'][product]:
                del query_results['products'][product]['counts']

        self._data = query_results

        files.remove('query_results.json')

        for file in files:
            content_type, _ = guess_type(file)

            if not content_type:
                logger.error(f'Cannot guess content type for file {file}, skipping')
                continue

            self._attachments.append(
                Attachment(
                    os.path.join(artifacts_dir, file),
                    file,
                    content_type=content_type,
                    content_disposition='INLINE',
                    content_id=Attachment.get_random_id('img')
                )
            )

    @classmethod
    def can_run(cls, check_token=False):
        token = cls._settings.get('GIT_PUB_ACCESS_TOKEN')

        if token is None:
            return False
        elif check_token:
            response = requests.get(
                'https://api.github.com',
                headers={'Authorization': f'token {token}'},
            )

            msg = f'Git token status check: {response.status_code}'

            if not response.ok:
                msg += f' {response.text}'

            logger.info(msg)

            return response.ok

        return True

    @staticmethod
    def _get_latest_sds_artifact(token=None):
        logger.info('Getting latest successful Github Actions run for the SDS query workflow')

        headers = {
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2026-03-10',
        }

        if token:
            headers['Authorization'] = f'Bearer {token}'

        runs_response = _get(
            f"https://api.github.com/repos/nasa/opera-sds/actions/workflows/{SDS_QUERY_WF_ID}/runs",
            params={
                'status': 'success',
                'per_page': 1
            },
            headers=headers
        )

        runs = runs_response.get('workflow_runs', [])

        if not runs:
            raise RuntimeError('No runs found')

        run = runs[0]

        logger.info(f'Found latest run: {run["id"]} finished at {run["updated_at"]} {run["html_url"]}')

        logger.info(f'Getting run artifacts for run ID {run["id"]}')

        artifacts_response = _get(
            f"https://api.github.com/repos/nasa/opera-sds/actions/runs/{run['id']}/artifacts",
            headers=headers
        )

        artifacts = artifacts_response.get('artifacts', [])

        if not artifacts:
            raise RuntimeError('No artifacts found')

        artifact = None

        for af in artifacts:
            if af['name'] == 'artifact':
                artifact = af
                break

        if not artifact:
            raise RuntimeError(f'Could not find expected artifact in {[af["name"] for af in artifacts]}')

        # TODO: Check expiry time (& also a recency cutoff check?)

        logger.info(f'Found artifact: {artifact["id"]} @ {artifact["archive_download_url"]}')

        return artifact['archive_download_url']
