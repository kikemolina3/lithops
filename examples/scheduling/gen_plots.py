from lithops.scheduler.scheduler import LithopsScheduler
from lithops.scheduler.use_cases import *


if __name__ == "__main__":
    # for i in stages_list:
    #     x = LithopsScheduler(i)
    #     x.draw()
    x = LithopsScheduler(example_stages)
    # evictions in 100 and 170
    # x.evict(100)
    x.draw()
    x.dump_to_csv()

