from typing import Union, Literal

Number = Union[int, float]
StringableT = Union[str, int, float]
AOFFsyncStrategy = Literal["always", "everysec"]
