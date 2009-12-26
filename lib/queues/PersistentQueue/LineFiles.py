"""
A specialized approach to serializing data for storage of PyCrawler
state in flat files.

LineFiles provides dump and load methods analogous to the marshal and
pickle modules.  Unlike those other serialization approaches,
LineFiles only serializes sequence type objects, such as lists,
tuples, and strings.  It also sorts them.
"""
# $Id: $
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

SORTABLE = True
SORT_FIELD = 1
DELIMITER = "|"

def make_record(line):
    """
    Converts a serialized line of characters into a record by
    splitting on the module's DELIMITER attribute.
    """
    return line.split(DELIMITER)

def get_sort_val(rec):
    """
    Uses the module's SORT_FIELD attribute to obtain a float out of
    part of 'rec'
    """
    val = float(rec[SORT_FIELD - 1])
    return val

def default_compare(x, y, get_sort_val=get_sort_val):
    """
    Used by LineFiles.dump to sort items before writing to disk.
    """
    x_val = get_sort_val(x)
    y_val = get_sort_val(y)
    if x_val > y_val:    return 1
    elif x_val == y_val: return 0
    else:                return -1 # x_val < y_val

def dump(items, output, compare=default_compare):
    """
    'items' must be a list.  This serializes each item, and writes it
    to the open 'output' file object.

    If 'compare' is not None, then 'items' is sorted using 'compare'
    as the comparison function.  Default is the module method
    'default_compare'.

    Each item is treated as a separate entity to serialize.  Entities
    of type list or tuple are serialized by joining with this module's
    DELIMITER attribute.
    """
    if compare is not None:
        items.sort(compare)
    for line in items:
        if isinstance(line, (tuple, list)):
            line = DELIMITER.join(line)
        output.write(line + "\n")

def load(input):
    """
    Loads into memory the full contents of the (already open) 'input'
    file object.  Treats each line as a separate entity to deserialize
    using LineFiles.make_record()

    Returns a list of these entities.
    """
    ret = []
    for line in input.readlines():
        line = line.strip()
        if line:
            ret.append(make_record(line))
    return ret
