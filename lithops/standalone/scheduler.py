#
# (C) Copyright Cloudlab URV 2020
# (C) Copyright IBM Corp. 2023
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
import math
import os
import time
import threading
import logging

import requests

from lithops.standalone import StandaloneHandler
from lithops.constants import SA_WORKER_SERVICE_PORT
from lithops.standalone.backends.aws_ec2.aws_ec2 import EC2Instance
from lithops.standalone.utils import VM_MEMORY_GB_DICT
logger = logging.getLogger(__name__)


class ProactiveScheduler(threading.Thread):
    def __init__(self, config, profiling):
        threading.Thread.__init__(self)

        self.standalone_config = config
        self.exec_mode = config['exec_mode']
        self.running = False

        self.standalone_handler = StandaloneHandler(self.standalone_config)
        self.instance_type = self.standalone_handler.backend.instance_type
        self.backend = self.standalone_handler.backend
        self.instances = []
        self.profiling = profiling
        self.current_offset = 0

    def run(self):
        self.running = True
        logger.info(f"Starting ProactiveScheduler monitor for {self.backend.name}")
        zero_tstamp = time.time()
        stages_to_create = set(stage['step'] for stage in self.profiling
                               if 'init_size' in stage and stage['init_size'] != 0)
        stages_to_kill = set(stage['step'] for stage in self.profiling
                             if 'kill_size' in stage and stage['kill_size'] != 0)

        while True:
            self.current_offset = time.time() - zero_tstamp
            # NOTE-SCHEDULING: create VMs when the time comes
            for index in stages_to_create.copy():
                stage = self.profiling[index]
                if 'init_size' in stage and stage['init_size'] != 0 and self.current_offset >= stage['vm_init_offset']:
                    self.create_instances(stage)
                    stages_to_create.remove(index)
            # NOTE-SCHEDULING: kill VMs when the time comes
            for index in stages_to_kill.copy():
                stage = self.profiling[index]
                if 'kill_size' in stage and stage['kill_size'] != 0 and self.profiling >= stage['fn_init_offset'] + \
                        stage['duration']:
                    # Check if functions over VMs finished and if so, kill VMs
                    threading.Thread(target=self.stop_instances, args=(stage,)).start()
                    stages_to_kill.remove(index)
            # Relax the CPU
            time.sleep(1)

    def stop_instances(self, stage):
        logger.debug(f"Starting to kill VMs for stage {stage['step']} in second {self.current_offset}")

        def is_worker_free(worker_private_ip):
            """
            Checks if the Lithops service is ready and free in the worker VM instance
            """
            url = f"http://{worker_private_ip}:{SA_WORKER_SERVICE_PORT}/ping"
            try:
                r = requests.get(url, timeout=0.5)
                resp = r.json()
                logger.debug(f'Worker processes status from {worker_private_ip}: {resp}')
                return True if resp.get('free', 0) > 0 else False
            except Exception:
                return False

        number_of_instances = math.ceil(stage['kill_size'] // VM_MEMORY_GB_DICT[self.instance_type])
        killed_instances = 0
        while killed_instances < number_of_instances:
            for instance in self.instances:
                if is_worker_free(instance.private_ip):
                    instance.delete()
                    self.instances.remove(instance)
                    killed_instances += 1
                    logger.debug(f"Killed instance {instance.name} ({instance.private_ip})")
            time.sleep(1)
        logger.debug(f"Killed {killed_instances} VMs for stage {stage['step']} in second {self.current_offset}")

    def create_instances(self, stage):
        number_of_instances = math.ceil(stage['init_size'] // VM_MEMORY_GB_DICT[self.instance_type])
        logger.debug(
            f"Creating {number_of_instances} VMs ({self.instance_type}) "
            f"for stage {stage['step']} in second {self.current_offset}")
        for _ in range(number_of_instances):
            instance = EC2Instance(self.backend, self.standalone_config)
            instance.create()
            self.instances.append(instance)
            logger.debug(f"Created instance {instance.name} ({instance.private_ip})")

    def eviction_handler(self):
        pass
