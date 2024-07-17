import os
from parsl.dataflow.memoization import id_for_memo
from parsl import File


@id_for_memo.register(File)
def id_for_memo_file(parsl_file_object: File, output_ref: bool = False) -> bytes:
    if output_ref and os.path.exists(parsl_file_object.filepath):
        return pickle.dumps(parsl_file_object.filepath)
