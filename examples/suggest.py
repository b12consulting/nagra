
# TODO !

def suggest(self, column, like=None):
    """
    Return a iterator over the possible values of columns. Use
    like in a where condition if given.
    """
    if "." not in column:
        raise NotImplementedError("TODO")
    local_col, remote_col = column.split(".", 1)
    cond = []
    if like:
        cond.append("(like " + remote_col + " {})")

    ftable = self.schema.get(self.foreign_keys[local_col])
    select = ftable.select(remote_col).where(*cond).groupby(remote_col).orderby(remote_col)
    if like:
        cur = execute(select.stm(), (like,))
        return (x for x, in cur)
    return (x for x, in select)
