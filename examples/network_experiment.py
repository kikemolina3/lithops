from concurrent.futures import ThreadPoolExecutor
import lithops
import time
from lithops import Storage

def map_function(x):
    storage = Storage()

    def calculate_ranges(i):
        # terasort-4g-*.parquet is 4GB
        # i want 1GB chunks x 4 threads
        # chunk_size = 1024 * 1024 * 1024
        chunk_size = 1024 * 1024 * 1024
        info = []
        file_index = i
        for j in range(4):
            start = j * chunk_size
            end = (j + 1) * chunk_size - 1
            info.append((file_index, start, end))
        return info
    
    def timed_get_file(i, ranges):
        start = time.time()
        # end_range = 50MB
        file_index, start_range, end_range = ranges
        result = storage.get_object(bucket='kubecaps-urv', key=f'terasort-4g-{file_index}.parquet', extra_get_args={'Range': f'bytes={start_range}-{end_range}'})
        end = time.time()
        print(f'File {i}: start={start:.3f}, end={end:.3f}, duration={end - start:.3f}')
        return {
            'start': start,
            'end': end,
            'duration': end - start,
            'file_size': len(result),
            'file_name': f'terasort-4g-{file_index}.parquet',
            'file_range': f'bytes={start_range}-{end_range}',
        }

    ranges = calculate_ranges(x)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(timed_get_file, i, j) for i, j in zip(range(4), ranges)]
        results = [f.result() for f in futures]

    return results


if __name__ == '__main__':
    fexec = lithops.FunctionExecutor()
    fexec.map(map_function, range(40))
    fexec.wait()
    results = fexec.get_result()

    import cloudpickle
    with open('40threads-r6in.large.pkl', 'wb') as f:
        cloudpickle.dump(results, f)
    exit(0)
