# -*- coding: utf-8 -*-
from enum import Enum
from secrets import token_bytes


class constants(Enum):
	JWT_SECRET_KEY = token_bytes(64).hex()
	JWT_ALGORITHM = "HS512"
	JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
