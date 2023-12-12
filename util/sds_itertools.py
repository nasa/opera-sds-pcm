"""Functions inspired by itertools and more_itertools for use by SDS"""


def windowed_by_predicate(iterable, pred, sorted_: bool = False, set_: bool = False):
    """
    This function will "window" an iterable using the given predicate. It additionally provides flags for sorting
    results and returning sets.

    An example of how to use this is to group an ordered set of timestamps by their time-proximity. E.g., given a list
    of timestamps, group into unique sets of timestamps that are within 5 minutes of one another.
    """
    groups = []
    for i, item in enumerate(iterable if sorted_ else sorted(iterable)):
        head, *tail = iterable[i:]
        a = head
        group = {a} if set_ else [a]
        for b in tail:
            if pred(a, b):
                group.add(b) if set_ else group.append(b)
        groups.append(group)
    return groups
