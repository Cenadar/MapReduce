import hashing


class AbstractShardingFunction:
    def __init__(self):
        pass

    def get_shard(self, key, **kwargs):
        raise NotImplementedError


class DefaultShardingFunction(AbstractShardingFunction):
    def __init__(self, shards):
        AbstractShardingFunction.__init__(self)
        assert shards > 0
        self.shards = shards

    def get_shard(self, key, **kwargs):
        return hashing.hash(key) % self.shards


class KeyAsShardingFunction(AbstractShardingFunction):
    def __init__(self):
        AbstractShardingFunction.__init__(self)

    def get_shard(self, key, **kwargs):
        return key
