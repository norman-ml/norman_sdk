from typing import Optional
from norman_objects.shared.security.sensitive import Sensitive


class CredentialsState:
    def __init__(
        self,
        account_id: Optional[str] = None,
        username: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self._account_id = account_id
        self._username = username
        self._email = email
        self._password = Sensitive(password) if password else None
        self._api_key = Sensitive(api_key) if api_key else None

    @property
    def account_id(self): return self._account_id
    @property
    def username(self): return self._username
    @property
    def email(self): return self._email
    @property
    def password(self): return self._password
    @property
    def api_key(self): return self._api_key

    def update(
        self,
        account_id: Optional[str] = None,
        username: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        if account_id is not None: self._account_id = account_id
        if username is not None: self._username = username
        if email is not None: self._email = email
        if password is not None: self._password = Sensitive(password)
        if api_key is not None: self._api_key = Sensitive(api_key)
