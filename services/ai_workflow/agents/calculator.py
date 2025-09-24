import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from simpleeval import SimpleEval, InvalidExpression
import math

_ALLOWED_CHARS_RE = re.compile(r"^[0-9\s\.\+\-\*\/\^\(\)\,%]+$")  # after normalization
_PERCENT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")

_ALLOWED_FUNCS = {
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    # Uncomment if you want them:
    # "floor": math.floor,
    # "ceil": math.ceil,
}

def _normalize_expression(expr: str) -> str:
    """
    Normalize friendly syntax to safe, evaluable form:
    - Replace '^' with '**'
    - Replace '12.5%' with '(12.5/100)'
    - Remove thousands separators like '1,234.56' -> '1234.56'
    - Strip whitespace
    """
    s = expr.strip()
    # Remove thousands separators only when between digits
    s = re.sub(r"(?<=\d),(?=\d)", "", s)
    # Convert caret power to Python power
    s = s.replace("^", "**")
    # Convert percent literals
    s = _PERCENT_RE.sub(r"(\1/100)", s)
    return s

def _validate_expression(expr: str) -> None:
    """Ensure only allowed characters remain after normalization."""
    if not _ALLOWED_CHARS_RE.match(expr):
        raise ValueError("Expression contains invalid characters or tokens.")

def _format_result(value: float, decimals: int) -> str:
    """
    Format using Decimal for stable rounding and nice trimming:
    - round half up
    - trim trailing zeros and dot
    """
    q = Decimal(10) ** -decimals
    d = Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP)
    s = f"{d:.{decimals}f}"
    # Trim trailing zeros and optional decimal point
    s = s.rstrip("0").rstrip(".")
    # Keep at least "0" for e.g. 0.000000 -> "0"
    return s if s != "" and s != "-0" else "0"

def calculate(expression: str, *, decimals: int = 6) -> str:
    """
    Evaluate a numeric expression safely and return the result as a string.

    Parameters
    ----------
    expression : str
        e.g. "(2.5 - 2.2) / 2.2 * 100", "1_000 + 2,000", "3^2", "12.5%"
    decimals : int
        Decimal places for rounding/formatting (default 6)

    Returns
    -------
    str
        Result, e.g. "13.636364" or "9" (with trimmed zeros). On error: "ERROR: <message>"
    """
    try:
        if not isinstance(expression, str):
            raise TypeError("Expression must be a string.")

        normalized = _normalize_expression(expression)
        _validate_expression(normalized)

        se = SimpleEval(
            names={},            # no variables allowed
            functions=_ALLOWED_FUNCS,
        )
        value = se.eval(normalized)  # may be int/float

        # Guard non-numeric outputs (shouldn't happen with our constraints)
        if not isinstance(value, (int, float)):
            raise InvalidExpression("Non-numeric result.")

        return _format_result(float(value), decimals=decimals)

    except ZeroDivisionError:
        return "ERROR: Division by zero"
    except InvalidExpression as e:
        return f"ERROR: Invalid expression ({e})"
    except Exception as e:
        # Generic safety net
        return f"ERROR: {str(e)}"

# --- Optional: quick self-tests (can be removed in production) ---
if __name__ == "__main__":
    tests = [
        "3 + 5*2",                    # 13
        "(2.5 - 2.2) / 2.2 * 100",    # 13.636364
        "1,234.5 + 2,000",            # 3234.5
        "3^2 + 4^2",                  # 25
        "12.5%",                      # 0.125
        "round((2/3)*100, 2)",        # 66.67
        "max(10, 20/3)",              # 10
        # "floor(3.7)",               # if enabled
        # "ceil(3.1)",                # if enabled
    ]
    for t in tests:
        print(t, "=>", calculate(t))
