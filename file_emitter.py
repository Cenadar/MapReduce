import base64
import pickle

from sharding_function import DefaultShardingFunction


class FilesEmitter:
    def __init__(self, output_file_names, sharding_function):
        if not sharding_function:
            sharding_function = DefaultShardingFunction(len(output_file_names))

        self.output_files = [open(file_name, 'w') for file_name in output_file_names]
        self.sharding_function = sharding_function

    def __del__(self):
        for file in self.output_files:
            file.close()

    def emit(self, key, value, **kwargs):
        shard = self.sharding_function.get_shard(key, **kwargs)
        file = self.output_files[shard]

        dumped = pickle.dumps(value)
        b64_encoded = base64.b64encode(dumped).decode('utf-8')

        file.write('{};{}\n'.format(key, b64_encoded))
        file.flush()


class FileEmitter(FilesEmitter):
    def __init__(self, output_file_name):
        super().__init__([output_file_name], None)
