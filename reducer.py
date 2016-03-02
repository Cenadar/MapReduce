class AbstractReducer:
    def __init__(self, emitter):
        self.emitter = emitter

    def reduce(self, key, values):
        raise NotImplementedError()

    def emit(self, key, value, **kwargs):
        self.emitter.emit(key, value, **kwargs)

class SumReducer(AbstractReducer):
    def __init__(self, emitter):
        super().__init__(emitter)

    def reduce(self, key, values):
        assert isinstance(values, list)

        sum = 0
        count = 0
        for value in values:
            sum += value[0]
            count += value[1]

        self.emit(key, (sum, count))


