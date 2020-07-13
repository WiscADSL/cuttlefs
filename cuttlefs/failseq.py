# TODO make this threadsafe if the file system starts using multiple threads
class FailSequence(object):
    """
    FailSequence is a string that determines whether a particular
    write should fail.

    It can only contain the following letters:
    x : Fail this access only
    w : Pass this access only
    X : Fail this and future accesses
    W : Pass this and future accesses

    example:
     w    x     x    w     X
     |    |     |    |     |
     +---------------+--------1st and 4th write should succeed
          |     |          |
          +-----+-------------2nd and 3rd write should fail
                           |
                           +--All other writes should fail
                           (If W, all other writes should pass)
    """

    __slots__ = ("idx", "seq", "end_idx", "termchar")

    def __init__(self, seq):
        self.seq = seq
        self._validate_seq()

        self.idx = -1
        self.termchar = self.seq[-1].lower()
        self.end_idx = len(seq) - 1

    def copy(self):
        c = FailSequence(self.seq)
        c.idx = self.idx
        return c

    def _validate_seq(self):
        errors = []

        allowed = {'x', 'w', 'W', 'X'}
        current = set(self.seq)
        unknown = current - allowed
        if len(unknown) > 0:
            errors.append("Unknown characters in sequence: %r", unknown)

        allowed = {'x', 'w'}
        current = set(self.seq[:-1])
        unknown = current - allowed
        if len(current) > 0 and len(unknown) > 0:
            errors.append("Only last character can be capitalized")

        if self.seq[-1] not in ('X', 'W'):
            errors.append("Sequence must terminate with X or W")

        if len(errors) == 0:
            return

        raise ValueError("\n".join([f"Found {len(errors)} errors"] + errors))

    def next(self):
        self.idx += 1
        if self.idx >= self.end_idx:
            # terminated
            return self.termchar

        return self.seq[self.idx]
