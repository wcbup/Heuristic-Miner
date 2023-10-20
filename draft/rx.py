from reactivex import create, of, operators as op
import reactivex


def push_five_strings(observer, scheduler):
    observer.on_next("Alpha")
    observer.on_next("Beta")
    observer.on_next("Gamma")
    observer.on_next("Delta")
    observer.on_next("Epsilon")
    observer.on_completed()


# # source = create(push_five_strings)
# source = of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")


# source.subscribe(
#     on_next=lambda i: print("Received {0}".format(i)),
#     on_error=lambda e: print("Error Occurred: {0}".format(e)),
#     on_completed=lambda: print("Done!"),
# )


# class Counter:
#     def __init__(self) -> None:
#         self.total_len = 0

#     def add_len(self, len: int) -> None:
#         self.total_len += len
# composed = source.pipe(op.map(lambda s: len(s)), op.filter(lambda i: i >= 5))
# composed.subscribe(lambda value: print("Received {0}".format(value)))
# counter = Counter()
# composed.subscribe(lambda value: counter.add_len(value))
# print(counter.total_len)


def length_more_than_5():
    # In v4 rx.pipe has been renamed to `compose`
    return reactivex.compose(
        op.map(lambda s: len(s)),
        op.filter(lambda i: i >= 5),
    )


reactivex.of("Alpha", "Beta", "Gamma", "Delta", "Epsilon").pipe(
    length_more_than_5()
).subscribe(lambda value: print("Received {0}".format(value)))


def lowercase():
    def _lowercase(source):
        def subscribe(observer, scheduler=None):
            def on_next(value):
                observer.on_next(value.lower())

            return source.subscribe(
                on_next, observer.on_error, observer.on_completed, scheduler=scheduler
            )

        return reactivex.create(subscribe)

    return _lowercase


reactivex.of("Alpha", "Beta", "Gamma", "Delta", "Epsilon").pipe(lowercase()).subscribe(
    lambda value: print("Received {0}".format(value))
)
