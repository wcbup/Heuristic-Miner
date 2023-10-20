from pybeamline.sources import log_source
from pybeamline.bevent import BEvent
from reactivex import Observable, operators

b_events = log_source("test.xes")
b_events = b_events.pipe(operators.take(3))


class Miner:
    def __init__(self) -> None:
        pass

    def get_new_event(self, event: BEvent) -> None:
        print(f"process name: {event.get_event_name()}")
        print(f" trace name: {event.get_trace_name()}")
        print(f" event name: {event.get_event_name()}")
        print(f" event time: {event.get_event_time()}")
        print(f" process attributes: {event.process_attributes}")
        print(f" trace attributes: {event.trace_attributes}")
        print(f" event attributes: {event.event_attributes}")

print()
miner = Miner()
b_events.subscribe(lambda x: miner.get_new_event(x))
