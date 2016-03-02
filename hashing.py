import hashlib


def hash(value):
    to_hash = str(value).encode('utf-8')
    hex_hash = hashlib.md5(to_hash).hexdigest()
    int_hash = int(hex_hash, 16)
    return int_hash