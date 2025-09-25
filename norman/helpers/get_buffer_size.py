import io
import os


def get_buffer_size(file_obj):
    if hasattr(file_obj, "fileno"):
        return os.fstat(file_obj.fileno()).st_size
    if isinstance(file_obj, io.BytesIO):
        return file_obj.getbuffer().nbytes
    raise ValueError("Unsupported file object or operation")
