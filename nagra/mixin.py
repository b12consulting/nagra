from nagra import execute, executemany


class Executable:
    def execute(self, *args):
        return execute(self.stm(), args)

    def executemany(self, args):
        return executemany(self.stm(), args)

    def __iter__(self):
        return iter(self.execute())
