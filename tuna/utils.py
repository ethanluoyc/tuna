import re


_find_unsafe = re.compile(r'[a-zA-Z0-9_^@%+=:,./-]').search

# https://github.com/chrissimpkins/shellescape/blob/master/lib/shellescape/main.py
def quote(s):
    """Return a shell-escaped version of the string *s*."""
    if not s:
        return "''"

    if _find_unsafe(s) is None:
        return s

    # use single quotes, and put single quotes into double quotes
    # the string $'b is then quoted as '$'"'"'b'

    return "'" + s.replace("'", "'\"'\"'") + "'"