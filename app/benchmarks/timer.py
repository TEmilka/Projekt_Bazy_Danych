import time

def measure(func,arg):
    start=time.perf_counter()
    func(arg)
    return time.perf_counter()-start