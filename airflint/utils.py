import re


def toidentifier(string: str) -> str:
    """
    Convert string to a python identifier.

    :param string: The string to convert to a valid python identifier.

    :return: the converted python identifier.
    """
    return re.sub(r"\W|^(?=\d)", "_", string)
