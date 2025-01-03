def parse_command_line(cmdline):
    parts = []
    current_part = ''
    in_quotes = False

    for char in cmdline:
        if char == '"' and not in_quotes:
            in_quotes = True
            continue
        elif char == '"' and in_quotes:
            in_quotes = False
            if current_part:
                parts.append(current_part)
                current_part = ''
            continue
        elif char.isspace() and not in_quotes:
            if current_part:
                parts.append(current_part)
                current_part = ''
            continue
        current_part += char

    if current_part:
        parts.append(current_part)

    if not parts:
        return None

    return parts


def combine_command_line(args):
    if not args:
        ""

    result = []
    for arg in args:
        arg = arg.strip()
        if " " in arg:
            arg = f'"{arg}"'
        result.append(arg)
    return " ".join(result)
