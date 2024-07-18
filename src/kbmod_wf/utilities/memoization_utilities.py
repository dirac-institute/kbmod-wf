import os
import pickle
from parsl.dataflow.memoization import id_for_memo
from parsl import File


@id_for_memo.register(File)
def id_for_memo_file(parsl_file_object: File, output_ref: bool = False) -> bytes:
    return pickle.dumps(parsl_file_object.filepath)
