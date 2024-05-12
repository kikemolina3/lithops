from lithops.scheduler.scheduler import LithopsScheduler
from lithops.scheduler.use_cases import hello_stages


if __name__ == "__main__":
    x = LithopsScheduler(hello_stages)
    x.evict(60)
    x.draw()
