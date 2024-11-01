"""
Given a JSON file containing a list of expressions, this script will parse each expression and output a JSON file containing an object with the parsed expressions.
Called from transformDocs.js.

Accepts one CLI argument: the path to the JSON file containing the expressions to parse.
"""

import ast
import json
import sys
import os

base_scalar_types = {
    "str",
    "int",
    "float",
    "bool",
    "bytearray",
    "timedelta",
    "None",
}


def parse_expression(ast_node, full_expression):
    """
    Turns the AST expression object into a typedoc-compliant dict
    """

    current_node_type = ast_node.__class__.__name__

    if current_node_type == "BinOp" and ast_node.op.__class__.__name__ == "BitOr":
        return {
            "type": "union",
            "types": [
                parse_expression(ast_node.left, full_expression),
                parse_expression(ast_node.right, full_expression),
            ],
        }

    if current_node_type == "Tuple":
        return [parse_expression(e, full_expression) for e in ast_node.elts]

    if current_node_type == "Subscript":
        if "id" in ast_node.value._fields and ast_node.value.id == "Annotated":
            return parse_expression(ast_node.slice.dims[0], full_expression)

        main_type = parse_expression(ast_node.value, full_expression)
        type_argument = parse_expression(ast_node.slice, full_expression)

        main_type["typeArguments"] = (
            type_argument if isinstance(type_argument, list) else [type_argument]
        )
        return main_type

    if current_node_type == "Constant":
        return {"type": "literal", "value": ast_node.value}

    # If the expression is not one of the types above, we simply print the expression
    return {
        "type": "reference",
        "name": full_expression[ast_node.col_offset : ast_node.end_col_offset],
    }


typedoc_types_path = sys.argv[1]

with open(typedoc_types_path, "r") as f:
    typedoc_out = {}
    expressions = f.read()

    expressions = json.loads(expressions)

    for expression in expressions:
        try:
            if typedoc_out.get(expression) is None:
                typedoc_out[expression] = parse_expression(
                    ast.parse(expression).body[0].value, expression
                )
        except Exception as e:
            print(f"Invalid expression encountered while parsing: {expression}")
            print(f"Error: {e}")

    with open(f"{os.path.splitext(typedoc_types_path)[0]}-parsed.json", "w") as f:
        f.write(json.dumps(typedoc_out, indent=4))
