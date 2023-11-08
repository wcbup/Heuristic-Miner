from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.sources import log_source

# log_source(["ABCD", "ABCD"]).pipe(
#     heuristics_miner_lossy_counting(model_update_frequency=4)
# ).subscribe(lambda x: print(str(x)))

log_source("extension-log-noisy-4.xes").pipe(
    heuristics_miner_lossy_counting(1000, 0.1,0.5,0.8)
).subscribe(lambda x: print(str(x)))