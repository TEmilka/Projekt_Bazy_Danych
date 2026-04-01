import time
import csv
import os

def run(db_name, scenario, func, data_iterable, output_file, dataset_size, repeats=3):
    times = []

    for i in range(repeats):
        start = time.time()

        ops = 0
        for item in data_iterable:
            func(item() if callable(item) else item)
            ops += 1

        end = time.time()
        total_time = end - start
        times.append(total_time)

    avg_time = sum(times) / len(times)

    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    file_exists = os.path.isfile(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "db",
                "scenario",
                "dataset_size",
                "time_sec"
            ])

        writer.writerow([
            db_name,
            scenario,
            dataset_size,
            round(avg_time, 6)
        ])

    print(
        f"{db_name} | {scenario} | size={dataset_size} | "
        f"AVG={avg_time:.6f}s | runs={times}"
    )