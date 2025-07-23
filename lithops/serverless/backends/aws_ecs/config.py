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
    'runtime_timeout': 3600,  # Default: 3600 seconds => 1 hour
    'runtime_memory': 512,  # Default memory: 512 MB
    'runtime_cpu': 0.25,
    'max_workers': 100,
    'worker_processes': 1,
    'invoke_pool_threads': 64,
    'architecture': 'x86_64',
    'ephemeral_storage': 21,  # Min 21GB in Fargate
    'env_vars': {},
    'user_tags': {},
    'vpc': {'subnets': [], 'security_groups': []},
    'efs': []
}

REQ_PARAMS = ('execution_role', 'security_groups', 'subnets')

RUNTIME_MEMORY_MIN = 512  # Min. memory: 512 MB
RUNTIME_MEMORY_MAX = 120*1024  # Max. memory: 120 GB

RUNTIME_TMP_SZ_MIN = 21  # Min. ephemeral storage: 21 GB
RUNTIME_TMP_SZ_MAX = 200  # Max. ephemeral storage: 200 GB


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

    cpu_memory_configs = {
        0.25: [512, 1024, 2048],
        0.5: list(range(1, 5)) * 1024,
        1: list(range(2, 9)) * 1024,
        2: list(range(4, 17)) * 1024,
        4: list(range(8, 31)) * 1024,
        8: list(range(16, 61)) * 1024,
        16: list(range(32, 121)) * 1024,
    }
    if config_data['aws_ecs']['runtime_cpu'] not in cpu_memory_configs:
        raise Exception(f"Invalid CPU value {config_data['aws_ecs']['runtime_cpu']} for AWS ECS backend. "
                        f"Valid values are: {list(cpu_memory_configs.keys())}")
    if config_data['aws_ecs']['runtime_memory'] not in cpu_memory_configs[config_data['aws_ecs']['runtime_cpu']]:
        raise Exception(f"Invalid memory value {config_data['aws_ecs']['runtime_memory']} for "
                        f"AWS ECS backend with {config_data['aws_ecs']['runtime_cpu']} CPU. "
                        f"Valid values are: {cpu_memory_configs[config_data['aws_ecs']['runtime_cpu']]}")

    if not isinstance(config_data['aws_ecs']['subnets'], list):
        raise Exception("Unknown type {} for 'aws_ecs/"
                        "vpc/subnet' section".format(type(config_data['aws_ecs']['subnets'])))

    if not isinstance(config_data['aws_ecs']['security_groups'], list):
        raise Exception("Unknown type {} for 'aws_ecs/"
                        "vpc/security_groups' section".format(type(config_data['aws_ecs']['security_groups'])))

    # Fargate runtime config
    if config_data['aws_ecs']['ephemeral_storage'] < RUNTIME_TMP_SZ_MIN \
            or config_data['aws_ecs']['ephemeral_storage'] > RUNTIME_TMP_SZ_MAX:
        raise Exception(f'Ephemeral storage value must be between {RUNTIME_TMP_SZ_MIN} and {RUNTIME_TMP_SZ_MAX}')

    if 'region_name' in config_data['aws_ecs']:
        config_data['aws_ecs']['region'] = config_data['aws_ecs'].pop('region_name')

    if 'region' not in config_data['aws_ecs']:
        raise Exception('"region" is mandatory under the "aws_ecs" or "aws" section of the configuration')
    elif 'region' not in config_data['aws']:
        config_data['aws']['region'] = config_data['aws_ecs']['region']
