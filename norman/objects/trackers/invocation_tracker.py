from typing import Literal, Optional, Callable

from norman_objects.shared.status_flags.status_flag import StatusFlag
from pydantic import BaseModel

_InvocationStage = Literal["Invocation", "Inputs_Upload", "Flags", "Results"]
_InvocationStatus = Literal["Starting", "Finished", "Waiting"]


class InvocationEvent(BaseModel):
    invocation_id: str
    model_id: str
    account_id: str
    stage: _InvocationStage
    status: _InvocationStatus

    is_flag_event: bool = False
    flags: Optional[list[StatusFlag]] = None


InvocationTracker = Callable[[InvocationEvent], None]
