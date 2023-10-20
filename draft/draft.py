from pybeamline.sources import string_test_source, xes_log_source_from_file, log_source
from pybeamline.mappers import (
    sliding_window_to_log,
    infinite_size_directly_follows_mapper,
)
from pybeamline.algorithms.discovery import (
    heuristics_miner_lossy_counting,
    heuristics_miner_lossy_counting_budget,
)
from pybeamline.algorithms.conformance import mine_behavioral_model_from_stream, behavioral_conformance
from pybeamline.filters import excludes_activity_filter, retains_activity_filter
from pm4py import discover_dfg_typed
import pm4py
from reactivex.operators import window_with_count

# b_events = string_test_source(["ABC", "ACB", "EFG"])
# # b_events = xes_log_source_from_file("test.xes")
# b_events.subscribe(lambda x: print(str(x)))
# print()

# b_events.pipe(
#     window_with_count(6),
#     sliding_window_to_log()
# ).subscribe(lambda log: print(discover_dfg_typed(log)))

# log = pm4py.read_xes("test.xes")
# print(log)
# print(type(log))

# b_events.pipe(retains_activity_filter("B")).subscribe(lambda x: print(str(x)))

# log_source(["ABC", "ACB"]).pipe(
#     infinite_size_directly_follows_mapper()
# ).subscribe(lambda x: print(x))

# log_source(["ABCD", "ABCD"]).pipe(
#     heuristics_miner_lossy_counting(model_update_frequency=1)
# ).subscribe(lambda x: print(str(x)))

# log_source(["ABCD", "ABCD"]).pipe(
#     heuristics_miner_lossy_counting_budget(model_update_frequency=4)
# ).subscribe(lambda x: print(str(x)))

# source = log_source(["ABCD", "ABCD"])
# reference_model = mine_behavioral_model_from_stream(source)
# print(reference_model)
# source.pipe(
#     excludes_activity_filter("A"),
#     behavioral_conformance(reference_model)
# ).subscribe(lambda x: print(str(x)))

def mine(log):
    print(pm4py.discover_dfg_typed(log))

log_source(["ABC", "ABD"]).pipe(
    window_with_count(8),
    sliding_window_to_log()
).subscribe(mine)