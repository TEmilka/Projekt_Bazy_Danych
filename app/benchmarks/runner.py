import time
import csv
import os

def run(db_name, scenario, func, data_iterable, output_file, batch_size=1):
    start = time.time()
    ops = 0

    batch = []

    for item in data_iterable:
        batch.append(item)

        if len(batch) == batch_size:
            if batch_size == 1:
                func(batch[0])
            else:
                func(batch)

            ops += len(batch)
            batch = []

    # reszta batcha
    if batch:
        if batch_size == 1:
            func(batch[0])
        else:
            func(batch)
        ops += len(batch)

    end = time.time()
    total_time = end - start

    ops_per_sec = ops / total_time if total_time > 0 else 0

    # zapis do CSV
    # ensure output directory exists
    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    file_exists = os.path.isfile(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(["db", "scenario", "ops", "time_sec", "ops_per_sec"])

        writer.writerow([db_name, scenario, ops, round(total_time, 4), round(ops_per_sec, 2)])

    print(f"{db_name} | {scenario} | ops={ops} | time={total_time:.2f}s | {ops_per_sec:.2f} ops/s")