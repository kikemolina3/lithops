#
# Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import copy
import os
import re
import logging
import boto3
import hashlib
import time
import json
import subprocess
import botocore.exceptions
import base64

from botocore.httpsession import URLLib3Session

from lithops import utils
from lithops.storage.utils import StorageNoSuchKeyError
from lithops.version import __version__
from lithops.constants import COMPUTE_CLI_MSG

from . import config as ecs_config

logger = logging.getLogger(__name__)

LITHOPS_TASK_ZIP = 'lithops_lambda.zip'


class AWSECSBackend:
    """
    A wrap-up around AWS Boto3 API
    """

    def __init__(self, ecs_config, internal_storage):
        """
        Initialize AWS Lambda Backend
        """
        logger.debug('Creating AWS ECS (Fargate) client')

        self.name = 'aws_ecs'
        self.type = utils.BackendType.BATCH.value
        self.ecs_config = ecs_config
        self.internal_storage = internal_storage
        self.user_agent = ecs_config['user_agent']
        self.region = ecs_config['region']
        self.role_arn = ecs_config['execution_role']
        self.namespace = ecs_config.get('namespace')

        self.aws_session = boto3.Session(
            aws_access_key_id=ecs_config.get('access_key_id'),
            aws_secret_access_key=ecs_config.get('secret_access_key'),
            aws_session_token=ecs_config.get('session_token'),
            region_name=self.region
        )

        self.ecs_client = self.aws_session.client(
            'ecs', config=botocore.client.Config(
                user_agent_extra=self.user_agent
            )
        )

        self.credentials = self.aws_session.get_credentials()
        self.session = URLLib3Session()
        self.host = f'ecs.{self.region}.amazonaws.com'

        if 'account_id' not in self.ecs_config or 'user_id' not in self.ecs_config:
            sts_client = self.aws_session.client('sts')
            identity = sts_client.get_caller_identity()

        self.account_id = self.ecs_config.get('account_id') or identity["Account"]
        self.user_id = self.ecs_config.get('user_id') or identity["UserId"]
        self.user_key = self.user_id.split(":")[0][-4:].lower()

        self.ecr_client = self.aws_session.client('ecr')
        package = f'lithops_v{__version__.replace(".", "")}_{self.user_key}'
        self.package = f"{package}_{self.namespace}" if self.namespace else package

        self._cluster_name = f'{self.package}_cluster'

        msg = COMPUTE_CLI_MSG.format('AWS ECS (Fargate)')
        if self.namespace:
            logger.info(f"{msg} - Region: {self.region} - Namespace: {self.namespace}")
        else:
            logger.info(f"{msg} - Region: {self.region}")

    def _format_task_name(self, runtime_name, runtime_memory, version=__version__):
        name = f'{runtime_name}-{runtime_memory}-{version}'
        name_hash = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
        task_name = f'lithops-worker-{self.user_key}-{version.replace(".", "")}-{name_hash}'
        return f'{self.namespace}-{task_name}' if self.namespace else task_name

    def _get_default_runtime_name(self):
        py_version = utils.CURRENT_PY_VERSION.replace('.', '')
        return f'default-runtime-v{py_version}'

    def _is_container_runtime(self, runtime_name):
        name = runtime_name.split('/', 1)[-1]
        return 'default-runtime-v' not in name

    def _format_repo_name(self, runtime_name):
        if ':' in runtime_name:
            base_image = runtime_name.split(':')[0]
        else:
            base_image = runtime_name
        return '/'.join([self.package, base_image]).lower()

    @staticmethod
    def _create_handler_bin(remove=True):
        """
        Create and return Lithops handler _ as zip bytes
        @param remove: True to delete the zip archive after building
        @return: Lithops handler _ as zip bytes
        """
        current_location = os.path.dirname(os.path.abspath(__file__))
        main_file = os.path.join(current_location, 'entry_point.py')
        utils.create_handler_zip(LITHOPS_TASK_ZIP, main_file, 'entry_point.py')

        with open(LITHOPS_TASK_ZIP, 'rb') as action_zip:
            action_bin = action_zip.read()

        if remove:
            os.remove(LITHOPS_TASK_ZIP)

        return action_bin

    def _wait_for_task_definition_ready(self, task_def_name):
        retries, sleep_seconds = (15, 25) if 'vpc' in self.ecs_config else (30, 5)

        while retries > 0:
            try:
                res = self.ecs_client.describe_task_definition(taskDefinition=task_def_name)
                if res['taskDefinition']['status'] == 'ACTIVE':
                    break
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'ClientException':
                    time.sleep(sleep_seconds)
                    retries -= 1
                    if retries == 0:
                        raise Exception(f'Task definition "{task_def_name}" not ready (timed out)')
                else:
                    raise

            logger.debug(f'"{task_def_name}" task definition is being registered...')
            time.sleep(sleep_seconds)
            retries -= 1

        logger.debug(f'Ok --> task definition "{task_def_name}" is active')

    def _delete_task_definition(self, task_def_name):
        logger.info(f'Deleting task definition: {task_def_name}')
        try:
            response = self.ecs_client.deregister_task_definition(
                taskDefinition=task_def_name
            )
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == 'ClientException' and 'is not a task definition' in \
                    err.response['Error']['Message']:
                logger.debug(f'OK --> Task definition {task_def_name} does not exist')
                return
            raise err

        if response['ResponseMetadata']['HTTPStatusCode'] in (200, 201):
            logger.debug(f'OK --> Deregistered task definition {task_def_name}')
        else:
            msg = f'An error occurred deregistering task definition {task_def_name}: {response}'
            raise Exception(msg)

    def _get_full_image_name(self, runtime_name):
        full_image_name = runtime_name if ':' in runtime_name else f'{runtime_name}:latest'
        registry = f'{self.account_id}.dkr.ecr.{self.region}.amazonaws.com'
        full_image_name = '/'.join([registry, self.package.replace('-', '.'), full_image_name]).lower()
        repo_name = full_image_name.split('/', 1)[1:].pop().split(':')[0]
        return full_image_name, registry, repo_name

    def build_runtime(self, runtime_name, runtime_file, extra_args=[]):
        logger.info(f'Building runtime {runtime_name} from {runtime_file}')

        docker_path = utils.get_docker_path()

        expression = '^([a-zA-Z0-9_-]+)(:[a-zA-Z0-9_-]+)+$'
        result = re.match(expression, runtime_name)

        if not result or result.group() != runtime_name:
            raise Exception(f"Runtime name must satisfy regex {expression}")

        res = self.ecr_client.get_authorization_token()
        if res['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Could not get ECR auth token: {res}')

        auth_data = res['authorizationData'].pop()
        ecr_token = base64.b64decode(auth_data['authorizationToken']).split(b':')[1]

        full_image_name, registry, repo_name = self._get_full_image_name(runtime_name)

        if runtime_file:
            assert os.path.isfile(runtime_file), f'Cannot locate "{runtime_file}"'
            cmd = f'{docker_path} build --platform=linux/amd64 -t {full_image_name} -f {runtime_file} . '
        else:
            cmd = f'{docker_path} build --platform=linux/amd64 -t {full_image_name} . '
        cmd = cmd + ' '.join(extra_args)

        try:
            entry_point = os.path.join(os.path.dirname(__file__), 'entry_point.py')
            utils.create_handler_zip(os.path.join(os.getcwd(), ecs_config.RUNTIME_ZIP), entry_point)
            utils.run_command(cmd)
        finally:
            os.remove(ecs_config.RUNTIME_ZIP)

        cmd = f'{docker_path} login --username AWS --password-stdin {registry}'
        subprocess.check_output(cmd.split(), input=ecr_token)

        try:
            self.ecr_client.create_repository(repositoryName=repo_name,
                                              imageTagMutability='MUTABLE')
        except self.ecr_client.exceptions.RepositoryAlreadyExistsException:
            logger.info('Repository {} already exists'.format(repo_name))

        logger.debug(f'Pushing runtime {full_image_name} to AWS container registry')
        cmd = f'{docker_path} push {full_image_name}'
        utils.run_command(cmd)

        logger.debug(f'Runtime {runtime_name} built successfully')

    def _deploy_container_runtime(self, runtime_name, memory, timeout):
        logger.info(f"Deploying runtime: {runtime_name} - Memory: {memory} Timeout: {timeout}")
        task_name = self._format_task_name(runtime_name, memory)

        if ':' in runtime_name:
            image, tag = runtime_name.split(':')
        else:
            image, tag = runtime_name, 'latest'

        try:
            repo_name = self._format_repo_name(image)
            response = self.ecr_client.describe_images(repositoryName=repo_name)
            images = response['imageDetails']
            if not images:
                raise Exception(f'Runtime {runtime_name} is not present in ECR. '
                                'Consider running "lithops runtime build -b aws_lambda ..."')
            image = list(filter(lambda image: 'imageTags' in image and tag in image['imageTags'], images)).pop()
            image_digest = image['imageDigest']
        except botocore.exceptions.ClientError:
            raise Exception(f'Runtime "{runtime_name}" is not deployed to ECR')

        registry = f'{self.account_id}.dkr.ecr.{self.region}.amazonaws.com'
        image_uri = f'{registry}/{repo_name}@{image_digest}'

        env_vars = {t['name']: t['value'] for t in self.ecs_config['env_vars']}

        container_def = {
            'name': task_name,
            'image': image_uri,
            'essential': True,
            'environment': [{'name': k, 'value': v} for k, v in env_vars.items()],
        }

        volumes = []
        mount_points = []
        for i, efs_conf in enumerate(self.ecs_config.get('efs', [])):
            vol_name = f'efs-{i}'
            volumes.append({
                'name': vol_name,
                'efsVolumeConfiguration': {
                    'fileSystemId': efs_conf['access_point'].split(':')[1],
                    'authorizationConfig': {
                        'accessPointId': efs_conf['access_point'].split('/')[-1],
                        'iam': 'ENABLED'
                    },
                    'transitEncryption': 'ENABLED'
                }
            })
            mount_points.append({
                'sourceVolume': vol_name,
                'containerPath': efs_conf['mount_path']
            })
        if mount_points:
            container_def['mountPoints'] = mount_points

        self._create_log_group()

        container_def['logConfiguration'] = {
            'logDriver': 'awslogs',
            'options': {
                'awslogs-group': '/ecs/lithops',
                'awslogs-region': 'us-east-1',
                'awslogs-stream-prefix': 'ecs'
            }
        }

        try:
            self.ecs_client.register_task_definition(
                family=task_name,
                requiresCompatibilities=['FARGATE'],
                cpu=str(int(self.ecs_config['runtime_cpu'] * 1024)),
                memory=str(self.ecs_config['runtime_memory']),
                networkMode='awsvpc',
                executionRoleArn=self.role_arn,
                containerDefinitions=[container_def],
                volumes=volumes,
                ephemeralStorage={'sizeInGiB': int(self.ecs_config['ephemeral_storage'])},
                tags=[
                    {'key': 'runtime_name', 'value': runtime_name},
                    {'key': 'lithops_version', 'value': __version__},
                    *[
                        {'key': k, 'value': v}
                        for k, v in self.ecs_config.get('user_tags', {}).items()
                    ]
                ],
            )
        except Exception as e:
            if 'ClientException' in str(e) and 'is already in use' in str(e):
                pass
            else:
                raise e

        try:
            self.ecs_client.create_cluster(clusterName=self._cluster_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ClusterAlreadyExistsException':
                logger.debug('Cluster "default" already exists')
            else:
                raise e

        self._wait_for_task_definition_ready(task_name)
        logger.debug(f'OK --> Created ECS task definition {task_name}')

    def _create_log_group(self):
        cloudwatch_client = self.aws_session.client('logs', region_name=self.region)
        log_group_name = '/ecs/lithops'
        try:
            cloudwatch_client.create_log_group(logGroupName=log_group_name)
        except Exception as e:
            if 'ResourceAlreadyExistsException' in str(e):
                logger.debug(f'Log group "{log_group_name}" already exists')
            else:
                raise e

    def deploy_runtime(self, runtime_name, memory, timeout):
        """
        Deploys a Lambda _ with the Lithops handler
        @param runtime_name: name of the runtime
        @param memory: runtime memory in MB
        @param timeout: runtime timeout in seconds
        @return: runtime metadata
        """
        self._deploy_container_runtime(runtime_name, memory, timeout)

        runtime_meta = self._generate_runtime_meta(runtime_name, memory)

        return runtime_meta

    def delete_runtime(self, runtime_name, runtime_memory, version=__version__):
        """
        Delete a Lithops lambda runtime
        @param runtime_name: name of the runtime to be deleted
        @param runtime_memory: memory of the runtime to be deleted in MB
        """
        logger.info(f'Deleting lambda runtime: {runtime_name} - {runtime_memory}MB')
        task_name = self._format_task_name(runtime_name, runtime_memory, version)

        self._delete_task_definition(task_name)

    def clean(self, **kwargs):
        logger.debug('Deleting all runtimes')

        prefix = f'{self.namespace}-lithops-worker-{self.user_key}' if self.namespace else f'lithops-worker-{self.user_key}'

        paginator = self.ecs_client.get_paginator('list_task_definitions')
        page_iterator = paginator.paginate(status='ACTIVE')

        for page in page_iterator:
            for task_def_arn in page['taskDefinitionArns']:
                task_def_name = task_def_arn.split('/')[-1]
                if task_def_name.startswith(prefix):
                    try:
                        self.ecs_client.deregister_task_definition(taskDefinition=task_def_arn)
                        logger.debug(f'Deregistered task definition {task_def_name}')
                    except botocore.exceptions.ClientError as e:
                        logger.error(f'Error deregistering task definition {task_def_name}: {str(e)}')

    def list_runtimes(self, runtime_name='all'):
        runtimes = []

        prefix = f'{self.namespace}-lithops-worker-{self.user_key}' if self.namespace else f'lithops-worker-{self.user_key}'

        paginator = self.ecs_client.get_paginator('list_task_definitions')
        page_iterator = paginator.paginate(status='ACTIVE')

        for page in page_iterator:
            for task_def_arn in page['taskDefinitionArns']:
                if not task_def_arn.split('/')[-1].startswith(prefix):
                    continue

                task_def = self.ecs_client.describe_task_definition(taskDefinition=task_def_arn)
                td = task_def['taskDefinition']
                memory = td.get('memory', None) or td['containerDefinitions'][0].get('memory', None)
                fn_name = td['taskDefinitionArn']

                tags_response = self.ecs_client.list_tags_for_resource(resourceArn=fn_name)
                tags = {tag['key']: tag['value'] for tag in tags_response.get('tags', [])}
                rt_name = tags.get('runtime_name')
                version = tags.get('lithops_version')

                runtimes.append((rt_name, memory, version, fn_name))

        if runtime_name != 'all':
            runtimes = [tup for tup in runtimes if runtime_name == tup[0]]

        return runtimes

    def invoke(self, runtime_name, runtime_memory, payload):
        executor_id = payload['executor_id']
        job_id = payload['job_id']
        total_calls = payload['total_calls']
        max_workers = payload['max_workers']
        chunksize = payload['chunksize']

        # Make sure only max_workers are started
        total_workers = total_calls // chunksize + (total_calls % chunksize > 0)
        if max_workers < total_workers:
            chunksize = total_calls // max_workers + (total_calls % max_workers > 0)
            total_workers = total_calls // chunksize + (total_calls % chunksize > 0)
            payload['chunksize'] = chunksize

        logger.debug(
            f'ExecutorID {executor_id} | JobID {job_id} - Required Workers: {total_workers}'
        )

        task_def_name = self._format_task_name(runtime_name, runtime_memory)

        internal_storage_config = copy.deepcopy(self.internal_storage.storage.config)

        overrides = {
            'containerOverrides': [{
                'name': task_def_name,
                'environment': [
                    {
                        'name': '__LITHOPS_ACTION',
                        'value': 'run_job'
                    },
                    {
                        'name': '__LITHOPS_PAYLOAD',
                        'value': json.dumps(payload)
                    },
                    {
                        'name': '__LITHOPS_CONFIG',
                        'value': json.dumps(internal_storage_config)
                    }
                ]
            }]
        }

        network_config = {
            'awsvpcConfiguration': {
                'subnets': self.ecs_config['subnets'],
                'securityGroups': self.ecs_config['security_groups'],
                'assignPublicIp': 'ENABLED'
            }
        }

        total_tasks = []

        # Invoking in batches of 10 tasks (ECS limit)
        for batch_index in range(0, total_workers, 10):
            batch_size = min(10, total_workers - batch_index)

            response = self.ecs_client.run_task(
                cluster=self._cluster_name,
                launchType='FARGATE',
                taskDefinition=task_def_name,
                overrides=overrides,
                networkConfiguration=network_config,
                count=batch_size,
            )

            tasks = response.get('tasks', [])
            if not tasks:
                raise Exception(f'No task started for task {task_def_name}: {response}')

            total_tasks.extend(tasks)

        activations_ids_key = f'{executor_id}-{job_id}-activations'
        task_arns = [task['taskArn'] for task in total_tasks]
        task_arns_dict = {}
        for task_id, task_arn in enumerate(task_arns):
            task_arns_dict[task_arn] = task_id
        self.internal_storage.put_data(activations_ids_key, json.dumps(task_arns_dict).encode('utf-8'))

    def get_runtime_key(self, runtime_name, runtime_memory, version=__version__):
        """
        Method that creates and returns the runtime key.
        Runtime keys are used to uniquely identify runtimes within the storage,
        in order to know which runtimes are installed and which not.
        """
        action_name = self._format_task_name(runtime_name, runtime_memory, version)
        runtime_key = os.path.join(self.name, version, self.region, action_name)

        return runtime_key

    def get_runtime_info(self):
        """
        Method that returns all the relevant information about the runtime set
        in config
        """
        if 'runtime' not in self.ecs_config or self.ecs_config['runtime'] == 'default':
            if utils.CURRENT_PY_VERSION not in ecs_config.AVAILABLE_PY_RUNTIMES:
                raise Exception(
                    f'Python {utils.CURRENT_PY_VERSION} is not available '
                    f'for AWS Lambda, please use one of {list(ecs_config.AVAILABLE_PY_RUNTIMES.keys())},'
                    ' or use a container runtime.'
                )
            self.ecs_config['runtime'] = self._get_default_runtime_name()

        runtime_info = {
            'runtime_name': self.ecs_config['runtime'],
            'runtime_memory': self.ecs_config['runtime_memory'],
            'runtime_timeout': self.ecs_config['runtime_timeout'],
            'max_workers': self.ecs_config['max_workers'],
        }

        return runtime_info

    def _generate_runtime_meta(self, runtime_name, runtime_memory):
        logger.debug(f'Extracting runtime metadata from: {runtime_name}')

        task_name = self._format_task_name(runtime_name, runtime_memory)

        payload = copy.deepcopy(self.internal_storage.storage.config)
        payload['runtime_name'] = runtime_name
        payload['log_level'] = logger.getEffectiveLevel()

        overrides = {
            'containerOverrides': [{
                'name': task_name,
                'environment': [
                    {
                        'name': '__LITHOPS_ACTION',
                        'value': 'get_metadata'
                    },
                    {
                        'name': '__LITHOPS_PAYLOAD',
                        'value': json.dumps(payload)
                    }
                ]
            }]
        }

        network_config = {
            'awsvpcConfiguration': {
                'subnets': self.ecs_config['subnets'],
                'securityGroups': self.ecs_config['security_groups'],
                'assignPublicIp': 'ENABLED'
            }
        }

        response = self.ecs_client.run_task(
            cluster=self._cluster_name,
            launchType='FARGATE',
            taskDefinition=task_name,
            overrides=overrides,
            networkConfiguration=network_config,
        )

        tasks = response.get('tasks', [])
        if not tasks:
            raise Exception(f'No task started for task {task_name}: {response}')

        logger.debug('Waiting for get-metadata job to finish')
        status_key = runtime_name + '.meta'
        retry = 0
        while retry < 10:
            logger.debug('Getting runtime metadata...')
            try:
                runtime_meta_json = self.internal_storage.get_data(key=status_key)
                runtime_meta = json.loads(runtime_meta_json)
                self.internal_storage.del_data(key=status_key)
                return runtime_meta
            except StorageNoSuchKeyError:
                time.sleep(20)
                retry += 1
        raise Exception('Could not get metadata')
