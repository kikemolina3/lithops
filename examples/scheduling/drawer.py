import pandas as pd
from matplotlib import pyplot as plt, patches

# Load the data
file_base = "demo"
stats = pd.read_csv(f'{file_base}.csv')
vms = pd.read_csv(f'{file_base}_vms.csv')
profiling = pd.read_csv(f'{file_base}_profiling.csv')

colors_list = ['navy', 'darkmagenta', 'darkorange', 'darkred', 'darkblue', 'darkviolet', 'darkgoldenrod', 'darkolivegreen', 'darkslategray']

# Get the start time of the first job
host_job_create_tstamp = min(vms['init_stamp'])
plot_width = max(stats['worker_end_tstamp'] - host_job_create_tstamp) * 1.05

# Get the maximum number of functions per stage
functions_per_stage = profiling['exec_size'] / 2
max_functions_per_stage = max(functions_per_stage)

# diff vms.init_tstamp - host_job_create_tstamp
vms['init_stamp'] = vms['init_stamp'] - host_job_create_tstamp
vms['end_stamp'] = vms['end_stamp'] - host_job_create_tstamp

vms['duration'] = vms['end_stamp'] - vms['init_stamp']
vms = vms.sort_values(by='duration', ascending=False)
vms = vms.reset_index(drop=True)

total_exec_time = max(stats['worker_end_tstamp'] - host_job_create_tstamp)
vm_instance_type = 'c5.large'
memory_per_fn = 2048


def get_my_stage(funcs_per_stage, func_index):
    # Get the stage of the function
    stage = 0
    for i, stage_size in enumerate(funcs_per_stage):
        if func_index < stage_size:
            return i
        func_index -= stage_size
    return -1


# Create the figure
fig = plt.figure(figsize=(10, 6))
fig.subplots_adjust(bottom=0.25)
ax = fig.add_subplot(111)

# Draw the VMs
for i, row in vms.iterrows():
    start_time = row['init_stamp']
    end_time = row['end_stamp']
    height = 2*0.97
    ax.add_patch(patches.Rectangle((start_time, i*2), end_time - start_time, height,
                                   color='green', alpha=0.2))
    # add red line of crosses for the end of the VM (but only in the right height) NOT axvline
    ax.add_patch(patches.Rectangle((end_time, i*2), plot_width/50, height,
                                   color='red', alpha=0.5, hatch='x'))
    # add blue line of crosses for the start of the VM (but only in the right height) NOT axvline
    ax.add_patch(patches.Rectangle((start_time, i*2), plot_width/50, height,
                                   color='dodgerblue', alpha=0.5, hatch='x'))


aux = 0
height = 0
# Draw the segments
for i, row in stats.iterrows():
    my_current_stage = get_my_stage(functions_per_stage, i)
    if my_current_stage != aux:
        aux = my_current_stage
        height = 0
    start_time = row['worker_start_tstamp'] - host_job_create_tstamp
    end_time = row['worker_end_tstamp'] - host_job_create_tstamp
    y = i
    ax.add_patch(patches.Rectangle((start_time, 0.2 +  height), end_time - start_time, 0.6,
                                   color=colors_list[get_my_stage(functions_per_stage, i)], alpha=0.6))
    height += 1

# set limits and plot
ax.set_xlim(0, plot_width)
ax.set_ylim(0, max_functions_per_stage)

# add legend with stages
patches_list = []
for i, stage in enumerate(functions_per_stage):
    patches_list.append(patches.Patch(color=colors_list[i], label=f'Stage {i}'))
plt.legend(handles=patches_list, loc='upper right', frameon=True)
# set title
plt.title(f'Execution {file_base.upper()}')
# set text area at bottom center
plt.figtext(0.3, 0.15, f'Total exec time = {total_exec_time:.2f}s', ha='center', va='center')
plt.figtext(0.3, 0.1, f'EC2 instance type = {vm_instance_type}', ha='center', va='center')
plt.figtext(0.3, 0.05, f'MB per func = {memory_per_fn}MB', ha='center', va='center')

plt.figtext(0.7, 0.15, f'Total cost = 0.0321$', ha='center', va='center')
plt.figtext(0.7, 0.1, f'FaaS cost = 0.456$', ha='center', va='center')
plt.figtext(0.7, 0.05, f'Number of evictions = 0', ha='center', va='center')


plt.show()