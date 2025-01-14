def parse_command_line(cmdline):
    if not cmdline or cmdline.isspace():
        return []

    parts = []
    current_part = ''
    in_quotes = False
    escape_next = False
    brace_count = 0
    quote_start = False  # Track if current part started with a quote

    for char in cmdline:
        # Handle escape character
        if char == '\\' and not escape_next:
            escape_next = True
            continue

        # Handle quotes
        if char == '"' and not escape_next:
            in_quotes = not in_quotes
            if len(current_part) == 0:  # Start of a part
                quote_start = True
                continue
            elif quote_start:  # End of a quoted part
                parts.append(current_part)
                current_part = ''
                quote_start = False
                continue
            current_part += char
            continue

        # Track JSON braces
        if char == '{':
            brace_count += 1
        elif char == '}':
            brace_count -= 1

        # Handle spaces - only split if we're not in quotes and not in JSON object
        if char.isspace() and not in_quotes and brace_count == 0:
            if current_part:
                parts.append(current_part)
                current_part = ''
            quote_start = False
            continue

        # Add character to current part
        current_part += char
        escape_next = False

    if current_part:
        parts.append(current_part)

    return parts


def combine_command_line(args):
    if not args:
        return ""

    result = []
    for arg in args:
        arg = arg.strip()
        if " " in arg or ('"' in arg or "{" in arg):  # Handle spaces and JSON
            # Escape any existing double quotes
            arg = arg.replace('"', '\\"')
            arg = f'"{arg}"'
        result.append(arg)
    return " ".join(result)
