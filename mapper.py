import hashing


class AbstractMapper:
    def __init__(self, emitter):
        self.emitter = emitter

    def map(self, value, **kwargs):
        raise NotImplementedError()

    def emit(self, key, value, **kwargs):
        self.emitter.emit(key, value, **kwargs)


class IdentityMapper(AbstractMapper):
    def __init__(self, unique_keys, emitter):
        AbstractMapper.__init__(self, emitter)
        self.unique_keys = unique_keys

    def map(self, value, **kwargs):
        file_name = kwargs.get('file_name')
        line_number = kwargs.get('line_number')

        key = hashing.hash((file_name, line_number)) % self.unique_keys
        key = '{:04}'.format(key)
        value = (int(value), 1)
        self.emit(key=key, value=value, **kwargs)

