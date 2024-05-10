from lithops.scheduler.scheduler import LithopsScheduler
from lithops.scheduler.use_cases import stages_list


if __name__ == "__main__":
    for i in stages_list:
        x = LithopsScheduler(i)
        x.draw()
