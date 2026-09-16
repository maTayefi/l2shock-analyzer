"""Double braces in the current clipboard text.

This is an explicitly invoked destructive clipboard utility. Importing this
module has no clipboard side effects.
"""

from __future__ import annotations


def double_braces(text: str) -> str:
    """Blindly double every opening and closing brace.

    This operation is intentionally not idempotent:
        {x}   -> {{x}}
        {{x}} -> {{{{x}}}}

    Callers should use it only when they deliberately want one additional level
    of brace escaping.
    """
    return str(text).replace("{", "{{").replace("}", "}}")


def main() -> int:
    try:
        import pyperclip
    except ImportError as exc:
        raise RuntimeError(
            "fixBrace.py requires the optional 'pyperclip' package."
        ) from exc

    original = pyperclip.paste()
    updated = double_braces(original)

    if updated == original:
        print("Clipboard unchanged: no braces found.")
        return 0

    pyperclip.copy(updated)
    print("Clipboard updated: every brace was doubled once.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
