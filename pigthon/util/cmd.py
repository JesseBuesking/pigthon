""" Command line utilities. """


def safe_quote(value):
    """
    Intelligently quotes the string supplied, doing the wacky single-double
    quote dance if a double quote is found in the string.

    See http://stackoverflow.com/a/1250279/435460 for more info.

    :param value:
    :type value: str or unicode
    :return: safely escaped string
    :retype: str
    """
    value = value.replace('"', '\'"\'"\'')
    if ' ' in value:
        value = '"' + value + '"'
    return value
