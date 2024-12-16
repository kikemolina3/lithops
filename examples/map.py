"""
Simple Lithops example using the map method.
In this example the map() method will launch one
map function for each entry in 'iterdata'. Finally
it will print the results for each invocation with
fexec.get_result()
"""
import lithops
import time


def my_map_function(id, x):
    print(f"I'm activation number {id}")
    time.sleep(20)
    return x + 7


if __name__ == "__main__":
    iterdata = list(range(20))
    fexec = lithops.FunctionExecutor()
    fexec.map(my_map_function, iterdata)
    print(fexec.get_result())
