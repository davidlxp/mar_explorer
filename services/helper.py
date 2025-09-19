# ------------------------------
# Some services's helper functions.
# Mostly used for development & debugging purposes.
# ------------------------------

import inspect

INDENT_INCREMENT = 2
DEFAULT_MAX_DEPTH = 3

def print_obj_attrs(obj, this_indent_cnt=0, max_list_items=5, max_depth=DEFAULT_MAX_DEPTH):
    """
    Print all available attributes and methods of an object.
    Recursively handles lists and dicts up to max_depth levels deep.
    """
    if this_indent_cnt // INDENT_INCREMENT >= max_depth:
        # print(" " * this_indent_cnt + "... (max depth reached)")
        return

    attrs = dir(obj)
    indent = " " * this_indent_cnt

    print(f"{indent}=== Attributes of {type(obj)} ===")
    for attr in attrs:
        if not attr.startswith("__"):  # skip dunder methods
            try:
                val = getattr(obj, attr)
                print(f"{indent}- {attr}: {type(val)}")

                if isinstance(val, list):
                    print_list_items(val, this_indent_cnt + INDENT_INCREMENT, max_list_items, max_depth)
                elif isinstance(val, dict):
                    print_dict_items(val, this_indent_cnt + INDENT_INCREMENT, max_depth)
                elif not isinstance(val, (str, int, float, bool, type(None))):
                    # Recurse into nested objects
                    print_obj_attrs(val, this_indent_cnt + INDENT_INCREMENT, max_list_items, max_depth)

            except Exception as e:
                print(f"{indent}- {attr}: <error retrieving> ({e})")


def print_list_items(the_list, this_indent_cnt=0, max_list_items=5, max_depth=DEFAULT_MAX_DEPTH):
    if this_indent_cnt // INDENT_INCREMENT >= max_depth:
        # print(" " * this_indent_cnt + "... (max depth reached)")
        return

    indent = " " * this_indent_cnt
    print(f"{indent}↳ list with {len(the_list)} items")

    for i, item in enumerate(the_list[:max_list_items]):
        print(f"{indent}  [{i}] {type(item)}")

        if isinstance(item, dict):
            print_dict_items(item, this_indent_cnt + INDENT_INCREMENT, max_depth)
        elif isinstance(item, list):
            print_list_items(item, this_indent_cnt + INDENT_INCREMENT, max_list_items, max_depth)
        elif not isinstance(item, (str, int, float, bool, type(None))):
            print_obj_attrs(item, this_indent_cnt + INDENT_INCREMENT, max_list_items, max_depth)


def print_dict_items(the_dict, this_indent_cnt=0, max_depth=DEFAULT_MAX_DEPTH):
    if this_indent_cnt // INDENT_INCREMENT >= max_depth:
        # print(" " * this_indent_cnt + "... (max depth reached)")
        return

    indent = " " * this_indent_cnt
    print(f"{indent}↳ dict with {len(the_dict)} keys")
    for k, v in the_dict.items():
        print(f"{indent}  {k}: {type(v)}")
        if isinstance(v, dict):
            print_dict_items(v, this_indent_cnt + INDENT_INCREMENT, max_depth)
        elif isinstance(v, list):
            print_list_items(v, this_indent_cnt + INDENT_INCREMENT, max_depth=max_depth)
        elif not isinstance(v, (str, int, float, bool, type(None))):
            print_obj_attrs(v, this_indent_cnt + INDENT_INCREMENT, max_depth=max_depth)
