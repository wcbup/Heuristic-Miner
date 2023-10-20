from reactivex import create, of, operators as op
import reactivex

source = of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")
# composed = source.pipe(op.map(lambda s: len(s)), op.filter(lambda i: i >= 5))
def print_value(value: str) -> None:
    print("Received {0}".format(value))
source.subscribe(lambda value: print("Received {0}".format(value)))
# source.subscribe(print_value)


class Counter:
    def __init__(self) -> None:
        self.total_len = 0

    def receive_string(self, string: str) -> None:
        print(f"receive: {string}, len: {len(string)}")
        self.total_len += len(string)
        print(f"total len: {self.total_len}")

print("---")

counter = Counter()
source.subscribe(lambda value: counter.receive_string(value))
print(counter.total_len)
