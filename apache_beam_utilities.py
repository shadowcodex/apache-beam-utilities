import apache_beam as beam

class InnerJoin(beam.PTransform):
    """Join two PCollections of <K,V> by inner join

    Keys must match on <K,V> pairs for join to work. Meaning that
    the PCollection passed in should have been prepared for join.

    Keyword arguments:
    pcoll -- <K,V> left side collection
    right -- <K,V> right side collection
    """

    def __init__(self, right):
        super(InnerJoin, self).__init__()
        self._right = right

    def _join(self, element):
        key, value = element
        left = value.get('left', None)
        right = value.get('right', None)

        if left is not None and len(left) > 0 and right is not None and len(right) > 0:                        
            left[0].update(right[0])
            return left
        else:
            return

    def expand(self, pcoll):
        return (
            { 'left': pcoll, 'right': self._right}
            | "Match" >> beam.CoGroupByKey()
            | "Combine" >> beam.FlatMap(self._join)
        )

class LeftOuterJoin(beam.PTransform):
    """Join two PCollections of <K,V> by left outer join

    Keys must match on <K,V> pairs for join to work. Meaning that
    the PCollection passed in should have been prepared for join.

    Keyword arguments:
    pcoll -- <K,V> left side collection
    right -- <K,V> right side collection
    """

    def __init__(self, right):
        super(LeftOuterJoin, self).__init__()
        self._right = right

    def _join(self, element):
        key, value = element
        left = value.get('left', None)
        right = value.get('right', None)

        if left is not None and len(left) > 0 and right is not None and len(right) > 0:                  
            left[0].update(right[0])
            return left
        elif left is not None and len(left) > 0:
            return left
        else:
            return

    def expand(self, pcoll):
        return (
            { 'left': pcoll, 'right': self._right}
            | "Match" >> beam.CoGroupByKey()
            | "Combine" >> beam.FlatMap(self._join)
        )

class RightOuterJoin(beam.PTransform):
    """Join two PCollections of <K,V> by right outer join

    Keys must match on <K,V> pairs for join to work. Meaning that
    the PCollection passed in should have been prepared for join.

    Keyword arguments:
    pcoll -- <K,V> left side collection
    right -- <K,V> right side collection
    """

    def __init__(self, right):
        super(RightOuterJoin, self).__init__()
        self._right = right

    def _join(self, element):
        key, value = element
        left = value.get('left', None)
        right = value.get('right', None)

        if left is not None and len(left) > 0 and right is not None and len(right) > 0:                  
            left[0].update(right[0])
            return left
        elif right is not None and len(right) > 0:
            return right
        else:
            return

    def expand(self, pcoll):
        return (
            { 'left': pcoll, 'right': self._right}
            | "Match" >> beam.CoGroupByKey()
            | "Combine" >> beam.FlatMap(self._join)
        )

class FullOuterJoin(beam.PTransform):
    """Join two PCollections of <K,V> by full outer join

    Keys must match on <K,V> pairs for join to work. Meaning that
    the PCollection passed in should have been prepared for join.

    Keyword arguments:
    pcoll -- <K,V> left side collection
    right -- <K,V> right side collection
    """

    def __init__(self, right):
        super(FullOuterJoin, self).__init__()
        self._right = right

    def _join(self, element):
        key, value = element
        left = value.get('left', None)
        right = value.get('right', None)

        if left is not None and len(left) > 0 and right is not None and len(right) > 0:                  
            left[0].update(right[0])
            return left
        elif left is not None and len(left) > 0:
            return left
        elif right is not None and len(right) > 0:
            return right
        else:
            return

    def expand(self, pcoll):
        return (
            { 'left': pcoll, 'right': self._right}
            | "Match" >> beam.CoGroupByKey()
            | "Combine" >> beam.FlatMap(self._join)
        )

class PrepareKey(beam.PTransform):
    """Convert Dictionary to <K,V> where K is a single or complex key required for a join.

    Can only work if value of element is a dictionary.

    Keyword arguments:
    pcoll - PCollection of values
    items - Comma separated string of items to create key from.
    """

    def __init__(self, items):
        super(PrepareKey, self).__init__()
        self._items = items.split(",")
        if len(self._items) == 0:
            raise Exception("No items specified for PrepareKey")

    def _parseKeyValue(self, element, items):
            """Helper function that does the actual transform"""
            key = []

            # if the item exists, put it in the key, otherwise throw an error because the key is not valid
            for item in items:
                if element.get(item, None) is not None:
                    key.append(element.get(item))
                else:
                    raise Exception("Key item does not exist in value, not valid for use with join.")
            return (key, element)

    def expand(self, pcoll):
        return (pcoll | beam.Map(self._parseKeyValue, self._items))



class Select(beam.PTransform):
    """Select certain elements from V of <K,V> PCollection. 
    
    Only works if value field is a dictionary.

    Keyword arguments:
    pcoll -- PCollection to filter values from.
    items -- Comma separated string of items to filter
    """

    def __init__(self, items, fillNull=False, nullValue=''):
        super(Select, self).__init__()
        self._items = items.split(",")
        self._nullValue = nullValue
        self._fillNull = fillNull
        if len(self._items) == 0:
            raise Exception("Not items specified for Filter")

    def _selectFilter(self, element, items):
        returnValues = {}
        for item in items:
            if element.get(item, None) is not None:
                returnValues[item] = element.get(item)     
            elif self._fillNull:
                returnValues[item] = self._nullValue
        if len(returnValues) > 0:
            return returnValues
        else:
            return    

    def expand(self, pcoll):
        return pcoll | "Select Map" >> beam.Map(self._selectFilter, self._items)