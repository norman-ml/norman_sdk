from typing import Literal, Optional, Callable

from norman_objects.shared.status_flags.status_flag import StatusFlag
from pydantic import BaseModel

_UploadStage = Literal["Model_Upload", "Inputs_Upload", "Flags"]
_UploadStatus = Literal["Starting", "Finished", "Waiting"]

class UploadEvent(BaseModel):
    model_id: str
    account_id: str
    stage: _UploadStage
    status: _UploadStatus

    is_flag_event: bool = False
    flags: Optional[list[StatusFlag]] = None


UploadTracker = Callable[[UploadEvent], None]
