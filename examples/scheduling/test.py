import lithops
import time

from lithops.scheduler.scheduler import LithopsScheduler
from lithops.scheduler.use_cases import hello_stages, first_stages


def my_map_function(id, x):
    print(f"I'm activation number {id}")
    print(f"Sleeping during {x} seconds")
    time.sleep(x)
    print("Sleeping done!")
    return None


if __name__ == "__main__":
    stages = hello_stages
    profiling = LithopsScheduler(stages).profiling
    fexec = lithops.FunctionExecutor(profiling=profiling)
    for index, step in enumerate(profiling):
        num_fn = stages[index]["num_fn"]
        params = [step["duration"] for i in range(int(num_fn))]
        result = fexec.map(my_map_function, params).get_result()
        print(result)
    fexec.plot(dst='.')
    fexec.dump_stats_to_csv("hello_stats")
