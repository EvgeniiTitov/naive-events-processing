import typing as t


class Chainer:
    """
    Calls provided functions in a sequence while passing results between them
    # TODO: Consider reporting when a function returns None
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
        if self._verbose:
            print(res)
        for i in range(len(self._remaining)):
            func = self._remaining[i]
            res = func(res)
            if self._verbose:
                print(res)
        return res


if __name__ == "__main__":
    f1 = lambda number: number * 2
    f2 = lambda number: number + 10

    def f3(number):
        return number / 100

    class F4:
        def __call__(self, number, *args, **kwargs):
            return number % 2 == 0

    chain = Chainer(functions=[f1, f2, f3, F4()], verbose=True)
    result = chain(100)
    print(result)
