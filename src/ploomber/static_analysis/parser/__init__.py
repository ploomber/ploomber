"""
This module contains code for lexical analysis and code parsing. It is used
to automatically extract "upstream" and "product" variables from notebooks.

Currently, Python parsing is delegated to the parso library. R parsing is done
via our own lexer and parser. Given that we only need to parse certain
statements, it does aim to implement a full R parser.

Our parsers only have to recognize a simple program, in Python, it is the
following:

upstream = ['a', 'b', 'c']
product = {'d': 'path/to/d.csv', 'e': 'path/to/e.csv'}

In R, this would be (note: <- and = are valid assignment operator):

upstream <- list('a', 'b', 'c')
product <- list(d='path/to/d.csv', e='path/to/d.csv')

The parser should be able to convert the R code above into the Python
representation.
"""
