def set_type(value):
    """
    Casts given value to either an int, float, or str

    :param value: The value to cast.

    :return: The value casted to either an int, float, or str.
    """
    constructors = [int, float, str]
    for c in constructors:
        try:
            return c(value)
        except ValueError:
            pass
    raise ValueError("{} is not one of these types: {}".format(c, constructors))
