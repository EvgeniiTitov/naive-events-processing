import typing as t


class Chainer:
    """
    Calls provided functions in a sequence while passing results between them
    """

    def __init__(
        self, functions: t.Sequence[t.Callable], verbose: bool = False
    ) -> None:
        if not len(functions):
            raise Exception("0 functions provided, nothing to chain")
        self._verbose = verbose
        self._funcs = tuple(functions)
        self._ensure_callables()
        self._first = self._funcs[0]
        self._remaining = self._funcs[1:]

    def _ensure_callables(self) -> None:
        for func in self._funcs:
            if not callable(func):
                raise Exception(f"Provided object {func} is not callable")

    def __call__(self, *args, **kwargs) -> t.Any:
        res = self._first(*args, **kwargs)
        if res is None:
            raise Exception("The first step did not return any results")
        if self._verbose:
            print(res)

        n_remaining = len(self._remaining)
        for i in range(n_remaining):
            func = self._remaining[i]
            res = func(res)
            if res is None and i != n_remaining - 1:
                raise Exception(
                    f"Intermediate step {func} did not " f"return any result"
                )
            if self._verbose:
                print(res)
        return res


if __name__ == "__main__":
    # f1 = lambda number: number * 2
    # f2 = lambda number: number + 10
    #
    # def f3(number):
    #     return number / 100
    #
    # class F4:
    #     def __call__(self, number, *args, **kwargs):
    #         return number % 2 == 0

    class F1:
        def __call__(self, value_in, *args, **kwargs):
            return value_in * 2

    class F2:
        def __call__(self, value_in, *args, **kwargs):
            return value_in - 100

    chain = Chainer(functions=[F1(), F2()], verbose=True)
    result = chain(100)

    print("\nFinal result:", result)
