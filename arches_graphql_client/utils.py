from functools import partial


def camel(string, studly=False):
    string = string.replace("& ", "and ").replace("-", " ")
    string = (
        (string[0].upper() if studly else string[0].lower())
        + string.replace("_", " ").title()[1:]
    ).replace(" ", "")
    if string[-1] != string[-1].lower():
        string += "_"
    return string


studly = partial(camel, studly=True)
