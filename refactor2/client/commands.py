from typing import Any

from refactor2.client.typing import ClientCommands


class BasicCommands(ClientCommands):
    def set(
            self,
            name: str,
            value: str,
            ex: int | None = None,
            px: int | None = None,
            nx: bool = False,
            xx: bool = False,
            keepttl: bool = False,
            get: bool = False,
            exat: int | None = None,
            pxat: int | None = None,
    ) -> Any:
        pieces: list = [name, value]
        if ex is not None:
            pieces.append("ex")
            pieces.append(ex)
        if px is not None:
            pieces.append("px")
            pieces.append(px)
        if exat is not None:
            pieces.append("exat")
            pieces.append(exat)
        if pxat is not None:
            pieces.append("pxat")
            pieces.append(pxat)

        if keepttl:
            pieces.append("keepttl")

        if nx:
            pieces.append("nx")
        if xx:
            pieces.append("xx")

        if get:
            pieces.append("get")

        return self.execute("set", *pieces)

    def get(self, name: str) -> Any:
        return self.execute("get", name)
