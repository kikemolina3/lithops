import math
from typing import Tuple

from matplotlib import pyplot as plt, patches
from matplotlib import lines as mlines

from lithops.scheduler.utils import EC2_INIT_TIME, EVICTION_TIME

def get_stage_by_tstamp(profiling, tstamp):
    for index, stage in enumerate(profiling):
        if stage['fn_init_offset'] <= tstamp <= stage['fn_init_offset'] + stage['duration']:
            return index
    return None

def my_startup_time(profiling, my_index):
    total_time = 0
    for i in range(my_index):
        total_time += profiling[i]['duration']
        if profiling[i]['exec_size'] == profiling[i]['init_size']:
            total_time += EC2_INIT_TIME
    return total_time


def unkill_previous_stage_window(profiling, stages, my_index):
    for i in range(my_index - 1, -1, -1):
        if (my_startup_time(profiling, my_index) - EC2_INIT_TIME <= my_startup_time(profiling, i) + profiling[i]['duration'] and
                stages[i]['size'] <= stages[my_index]['size']):
            profiling[i]['kill_size'] = 0
        else:
            break


def search_previous_stage_window(profiling, stages, my_index) -> Tuple[int, int]:
    stage_index, stage_size = -1, -1
    for i in range(my_index-1, -1, -1):
        if my_startup_time(profiling, my_index) - EC2_INIT_TIME <= my_startup_time(profiling, i) + profiling[i]['duration'] \
                and stages[i]['size'] > stage_size and stage_size <= stages[my_index]['size']:
            stage_index = i
            stage_size = profiling[i]['exec_size']
    return stage_index, stage_size


class LithopsScheduler:
    def __init__(self, stages):
        self.stages = stages
        self.profiling = None
        self.evictions = []
        self._preprocess_stages()
        self._generate_profiling()
        pass

    def _preprocess_stages(self):
        for index, stage in enumerate(self.stages):
            if 'memory' not in stage:
                stage['memory'] = 256
            stage['size'] = math.ceil(stage['num_fn'] * stage['memory'] / 1024)
            stage['step'] = index

    def _generate_profiling(self):
        self.profiling = []
        for index, stage in enumerate(self.stages):
            stage_output = {'service': 'spot', 'exec_size': stage['size'], 'duration': stage['duration'],
                            'step': stage['step']}
            if index == len(self.stages) - 1:
                stage_output.update({
                    'kill_size': stage['size']
                })
            if index == 0:  # first stage
                stage_output.update({
                    'init_size': stage['size'],
                    'vm_init_offset': my_startup_time(self.profiling, index),
                    'fn_init_offset': my_startup_time(self.profiling, index) + EC2_INIT_TIME,
                })
            else:  # some warmed vms
                stage_output.update({'fn_init_offset': my_startup_time(self.profiling, index)})
                unkill_previous_stage_window(self.profiling, self.stages, index)
                warmed_index, warmed_size = search_previous_stage_window(self.profiling, self.stages, index)
                if warmed_size <= stage['size']:  # scale up
                    self.profiling[warmed_index]['kill_size'] = 0
                    stage_output.update({
                        'init_size': stage['size'] - warmed_size,
                        'vm_init_offset': my_startup_time(self.profiling, index) - EC2_INIT_TIME,
                    })
                else:  # scale down
                    self.profiling[warmed_index]['kill_size'] = warmed_size - stage['size']
                    stage_output.update({
                        'init_size': 0,
                        'vm_init_offset': None,
                    })
            self.profiling.append(stage_output)

    def draw(self):
        # wallclock diagram with stages as rectangle (x is time, y is size)
        # orange for aws lambda, yellow for on-demand, green for spot

        fig, ax = plt.subplots()

        for index, stage in enumerate(self.profiling):
            color = 'lightsteelblue'
            ax.add_patch(
                patches.Rectangle(
                    (stage['fn_init_offset'], 0),  # (x,y)
                    stage['duration'],  # width
                    stage['exec_size'],  # height
                    edgecolor='black',
                    facecolor=color,
                    label="Computation"
                )
            )

        viewport_width = self.profiling[-1]['fn_init_offset'] + self.profiling[-1]['duration']
        height_diagram = max([stage['exec_size'] for stage in self.profiling])

        # draw transparent blue rectangle for the startup time in on-demands machines
        for stage in self.profiling:
            if stage['service'] != 'lambda' and stage['vm_init_offset'] is not None:
                ax.add_patch(
                    patches.Rectangle(
                        (stage['vm_init_offset'], stage['exec_size'] -
                         stage['init_size']),  # (x,y)
                        EC2_INIT_TIME,  # width
                        stage['init_size'],  # height
                        edgecolor='black',
                        facecolor='greenyellow',
                        alpha=0.5,
                        hatch='//',
                        label='VMs startup'
                    )
                )
            if stage['service'] != 'lambda' and "kill_size" in stage and stage['kill_size'] > 0:
                ax.add_patch(
                    patches.Rectangle(
                        (stage['fn_init_offset'] + stage['duration'] -
                         viewport_width / 80, stage['exec_size'] - stage['kill_size']),  # (x,y)
                        viewport_width / 40,  # width
                        stage['kill_size'],  # height
                        edgecolor='black',
                        facecolor='red',
                        alpha=0.5,
                        hatch='xx',
                        label='VMs shutdown'
                    )
                )

        for eviction in self.evictions:
            ax.add_patch(
                patches.Rectangle(
                    (eviction['tstamp'], -(height_diagram * 0.05)),  # (x,y)
                    EC2_INIT_TIME,  # width
                    (height_diagram * 0.04),  # height
                    edgecolor='black',
                    facecolor='orange',
                    alpha=0.5,
                    hatch='++',
                    label='Recovering VMs'
                )
            )
            # VERTICAL orange stripped line from 0 to the top of the diagram in eviction time
            ax.add_line(
                mlines.Line2D([eviction['tstamp'], eviction['tstamp']],
                              [-(height_diagram * 0.05), max([stage['exec_size'] for stage in self.profiling])],
                              color='orange', linestyle='--')
            )

        ax.set_xlim([0, viewport_width])
        ax.set_ylim([-(height_diagram * 0.05), height_diagram])

        handles, labels = ax.get_legend_handles_labels()
        unique_labels = []
        unique_handles = []

        for handle, label in zip(handles, labels):
            if label not in unique_labels:
                unique_labels.append(label)
                unique_handles.append(handle)

        ax.legend(unique_handles, unique_labels)
        ax.set_xlabel('Wallclock time (s)')
        ax.set_ylabel('# of functions')

        plt.show()

    def evict(self, offset):
        self.evictions.append({'tstamp': offset})
        eviction_offset = offset
        stage_index = get_stage_by_tstamp(self.profiling, eviction_offset)
        # check if stage is finishable or not
        finishable = (self.profiling[stage_index]['fn_init_offset'] +
                      self.profiling[stage_index]['duration'] <= eviction_offset + EVICTION_TIME)
        if finishable:
            if stage_index < len(self.profiling) - 1:
                offset = max(eviction_offset + EC2_INIT_TIME - self.profiling[stage_index + 1]['fn_init_offset'], my_startup_time(self.profiling, stage_index + 1) - self.profiling[stage_index]['duration'] - offset)
            for i in range(stage_index + 1, len(self.profiling)):
                self.profiling[i]['fn_init_offset'] += offset
                if 'vm_init_offset' in self.profiling[i] and self.profiling[i]['vm_init_offset'] is not None:
                    self.profiling[i]['vm_init_offset'] += offset
        else:
            if stage_index < len(self.profiling) - 1:
                offset = eviction_offset + EC2_INIT_TIME - self.profiling[stage_index + 1]['fn_init_offset'] + \
                         self.profiling[stage_index]['duration']
                # kill all vms on my stage
                self.profiling[stage_index]['kill_size'] = self.profiling[stage_index]['exec_size']
                # make that next stage init all vms again
                self.profiling[stage_index + 1]['init_size'] = self.profiling[stage_index + 1]['exec_size']
                if ('vm_init_offset' not in self.profiling[stage_index + 1] or
                        self.profiling[stage_index + 1]['vm_init_offset'] is None):
                    self.profiling[stage_index + 1]['vm_init_offset'] = my_startup_time(self.profiling,
                                                                                        stage_index + 1) - EC2_INIT_TIME
            for i in range(stage_index + 1, len(self.profiling)):
                self.profiling[i]['fn_init_offset'] += offset
                if 'vm_init_offset' in self.profiling[i] and self.profiling[i]['vm_init_offset'] is not None:
                    self.profiling[i]['vm_init_offset'] += offset
