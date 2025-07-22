#
# Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import copy
import logging

RUNTIME_ZIP = 'lithops_aws_ecs.zip'

logger = logging.getLogger(__name__)

DEFAULT_REQUIREMENTS = [
    'numpy',
    'requests',
    'redis',
    'pika',
    'cloudpickle',
    'ps-mem',
    'tblib',
    'urllib3<2',
    'psutil'
]

AVAILABLE_PY_RUNTIMES = {
    '3.6': 'python3.6',
    '3.7': 'python3.7',
    '3.8': 'python3.8',
    '3.9': 'python3.9',
    '3.10': 'python3.10',
    '3.11': 'python3.11',
    '3.12': 'python3.12'
}

USER_RUNTIME_PREFIX = 'lithops.user_runtimes'

DEFAULT_CONFIG_KEYS = {
    'runtime_timeout': 180,  # Default: 3600 seconds => 1 hour
    'runtime_memory': 256,  # Default memory: 256 MB
    'max_workers': 100,
    'worker_processes': 1,
    'invoke_pool_threads': 64,
    'architecture': 'x86_64',
    # TODO: control ephemeral storage
    'ephemeral_storage': 512,
    'env_vars': {},
    'user_tags': {},
    'vpc': {'subnets': [], 'security_groups': []},
    'efs': []
}

REQ_PARAMS = ('execution_role',)

# RUNTIME_TIMEOUT_MAX = No timeout

# TODO: set ECS Fargate values
RUNTIME_MEMORY_MIN = 128  # Max. memory: 128 MB
RUNTIME_MEMORY_MAX = 10240  # Max. memory: 10240 MB

RUNTIME_TMP_SZ_MIN = 512
RUNTIME_TMP_SZ_MAX = 10240


# TODO: adapt to aws_ecs specs
def load_config(config_data):

    if not config_data['aws_ecs']:
        raise Exception("'aws_ecs' section is mandatory in the configuration")

    if 'aws' not in config_data:
        config_data['aws'] = {}

    temp = copy.deepcopy(config_data['aws_ecs'])
    config_data['aws_ecs'].update(config_data['aws'])
    config_data['aws_ecs'].update(temp)

    for param in REQ_PARAMS:
        if param not in config_data['aws_ecs']:
            msg = f'"{param}" is mandatory in the "aws_ecs" section of the configuration'
            raise Exception(msg)

    for key in DEFAULT_CONFIG_KEYS:
        if key not in config_data['aws_ecs']:
            config_data['aws_ecs'][key] = DEFAULT_CONFIG_KEYS[key]

    if config_data['aws_ecs']['runtime_memory'] > RUNTIME_MEMORY_MAX:
        logger.warning("Memory set to {} - {} exceeds "
                       "the maximum amount".format(RUNTIME_MEMORY_MAX, config_data['aws_ecs']['runtime_memory']))
        config_data['aws_ecs']['runtime_memory'] = RUNTIME_MEMORY_MAX

    if config_data['aws_ecs']['runtime_memory'] < RUNTIME_MEMORY_MIN:
        logger.warning("Memory set to {} - {} is lower than "
                       "the minimum amount".format(RUNTIME_MEMORY_MIN, config_data['aws_ecs']['runtime_memory']))
        config_data['aws_ecs']['runtime_memory'] = RUNTIME_MEMORY_MIN

    # if config_data['aws_ecs']['runtime_timeout'] > RUNTIME_TIMEOUT_MAX:
    #     logger.warning("Timeout set to {} - {} exceeds the "
    #                    "maximum amount".format(RUNTIME_TIMEOUT_MAX, config_data['aws_ecs']['runtime_timeout']))
    #     config_data['aws_ecs']['runtime_timeout'] = RUNTIME_TIMEOUT_MAX

    if not {'subnets', 'security_groups'}.issubset(set(config_data['aws_ecs']['vpc'])):
        raise Exception("'subnets' and 'security_groups' are mandatory sections under 'aws_ecs/vpc'")

    if not isinstance(config_data['aws_ecs']['vpc']['subnets'], list):
        raise Exception("Unknown type {} for 'aws_ecs/"
                        "vpc/subnet' section".format(type(config_data['aws_ecs']['vpc']['subnets'])))

    if not isinstance(config_data['aws_ecs']['vpc']['security_groups'], list):
        raise Exception("Unknown type {} for 'aws_ecs/"
                        "vpc/security_groups' section".format(type(config_data['aws_ecs']['vpc']['security_groups'])))

    if not isinstance(config_data['aws_ecs']['efs'], list):
        raise Exception("Unknown type {} for "
                        "'aws_ecs/efs' section".format(type(config_data['aws_ecs']['vpc']['security_groups'])))

    if not all(
            ['access_point' in efs_conf and 'mount_path' in efs_conf for efs_conf in config_data['aws_ecs']['efs']]):
        raise Exception("List of 'access_point' and 'mount_path' mandatory in 'aws_ecs/efs section'")

    if not all([efs_conf['mount_path'].startswith('/mnt') for efs_conf in config_data['aws_ecs']['efs']]):
        raise Exception("All mount paths must start with '/mnt' on 'aws_ecs/efs/*/mount_path' section")

    # Lambda runtime config
    if config_data['aws_ecs']['ephemeral_storage'] < RUNTIME_TMP_SZ_MIN \
            or config_data['aws_ecs']['ephemeral_storage'] > RUNTIME_TMP_SZ_MAX:
        raise Exception(f'Ephemeral storage value must be between {RUNTIME_TMP_SZ_MIN} and {RUNTIME_TMP_SZ_MAX}')

    if 'region_name' in config_data['aws_ecs']:
        config_data['aws_ecs']['region'] = config_data['aws_ecs'].pop('region_name')

    if 'region' not in config_data['aws_ecs']:
        raise Exception('"region" is mandatory under the "aws_ecs" or "aws" section of the configuration')
    elif 'region' not in config_data['aws']:
        config_data['aws']['region'] = config_data['aws_ecs']['region']
