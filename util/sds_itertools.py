"""Functions inspired by itertools and more_itertools for use by SDS"""


def windowed_by_predicate(iterable, pred, sorted_: bool = False, set_: bool = False):
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
