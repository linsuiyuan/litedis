from refactor.client import BaseClient
from refactor.server import CommandFactory
from refactor.typing import StringLikeT, KeyT


class BasicCmds(BaseClient):

    def append(self, key: KeyT, value: StringLikeT) -> int:
        """
        将一个字符串值追加到已存在的键的末尾

        :return: 追加后的字符串的长度
        """
        strcmd = f'append "{key}" "{value}"'
        cmd = CommandFactory.create_cmd_from_str(self.db, strcmd)

        return cmd.execute()

    def copy(self, source: KeyT, destination: KeyT, replace: bool = False) -> bool:
        """
        复制一个键及其值到另一个键

        :return: 返回 True 表示复制成功，返回 False 表示源键不存在
        """

        strcmd = f'copy "{source}" "{destination}"'
        if replace:
            strcmd += 'replace'
        cmd = CommandFactory.create_cmd_from_str(self.db, strcmd)

        result = cmd.execute()

        return result == 1
