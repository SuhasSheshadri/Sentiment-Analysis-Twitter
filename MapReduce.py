class MapReduce:
    def __init__(self):
        # initialize dictionary for intermediate values from Map task
        self.intermediate = {}
        # initialize list for results of Reduce task
        self.result = []

    def emit_intermediate(self, key, value):
        # if key not already in dictionary, set value to empty list
        self.intermediate.setdefault(key, [])
        # add value to list associated with key
        self.intermediate[key].append(value)

    def emit(self, value):
        # append value to list of results
        self.result.append(value) 

    def execute(self, data, mapper, reducer):
        # read each line from input file; call Map function on each record
        for line in data:
            line = line.split(",")
            mapper(line)
        
        # for each key:value list in intermediate dictionary, call Reduce task
        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        self.result.sort()
        # print all the results
        print(self.result)
        print("\nThe format for the Results are: \n")
        print("Sentiment Analysis: (Tweet number, Sentiment Score)\n")
        print("Tfdf: (Word, Number of occurrences, [(Tweet number, Frequency in this Tweet)])\n")