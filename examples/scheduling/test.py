import lithops
import time

from lithops.scheduler.scheduler import LithopsScheduler
from lithops.scheduler.use_cases import stages_list


def my_map_function(id, x):
    print(f"I'm activation number {id}")
    print(f"Sleeping during {x} seconds")
    time.sleep(x)
    print("Sleeping done!")
    return None


if __name__ == "__main__":
    # fexec = lithops.FunctionExecutor(profiling=first_profiling)
    # for step in first_profiling:
    #     num_fn = step["exec_size"] // 2
    #     params = [step["duration"] for i in range(int(num_fn))]
    #     result = fexec.map(my_map_function, params).get_result()
    #     print(result)
    # fexec.plot(dst='.')
    # stages_list is list of all vars in use_cases.py
    for i in stages_list:
        x = LithopsScheduler(i)
        x.draw()
