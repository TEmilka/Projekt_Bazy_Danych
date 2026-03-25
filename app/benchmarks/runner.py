import csv
from benchmarks.timer import measure

def run(db,scenario,func,data,file):
    times=[measure(func,x) for x in data]
    avg=sum(times)/len(times)

    with open(file,"a",newline="") as f:
        csv.writer(f).writerow([scenario,len(data),avg])