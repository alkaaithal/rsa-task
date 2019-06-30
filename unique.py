def unique2(l):
    check = set()
    new_list = []
    for d in l:
        t = tuple(d.items())
        if t not in check:
            check.add(t)
            new_list.append(d)

    return new_list
