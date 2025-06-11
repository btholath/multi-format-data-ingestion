"""
recordio_utils.py

Utility functions for reading and writing MXNet RecordIO files.
"""

import mxnet as mx
import numpy as np

def write_recordio(arrays, filename):
    """Write a list of numpy arrays to a RecordIO file."""
    writer = mx.recordio.MXRecordIO(filename, 'w')
    for arr in arrays:
        s = mx.recordio.pack(mx.recordio.IRHeader(0, 0, 0, 0), arr.tobytes())
        writer.write(s)
    writer.close()
    print(f"Wrote {filename}")

def read_recordio(filename, dtype=np.int64):
    """Read a RecordIO file and return a list of numpy arrays."""
    reader = mx.recordio.MXRecordIO(filename, 'r')
    arrays = []
    while True:
        item = reader.read()
        if item is None:
            break
        _, data = mx.recordio.unpack(item)
        arr = np.frombuffer(data, dtype=dtype)
        arrays.append(arr)
    return arrays
