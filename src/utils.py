import hashlib


def hash_record(record):
    values = [str(record.title), str(record.year), str(record.metascore)]
    values_str = "".join(values)
    return hashlib.md5(values_str.encode()).hexdigest()
