from refactor.client import BaseClient
from refactor.typing import StringLikeT, KeyT


class BasicCmds(BaseClient):

    def append(self, key: KeyT, value: StringLikeT) -> int:
        """
        将一个字符串值追加到已存在的键的末尾

        :return: 追加后的字符串的长度
        """
        strcmd = f'append "{key}" "{value}"'

        return self.process_command(strcmd)

    def copy(self, source: KeyT, destination: KeyT, replace: bool = False) -> bool:
        """
        复制一个键及其值到另一个键

        :return: 返回 True 表示复制成功，返回 False 表示源键不存在
        """

        strcmd = f'copy "{source}" "{destination}"'
        if replace:
            strcmd += 'replace'

        result = self.process_command(strcmd)

        return result == 1


