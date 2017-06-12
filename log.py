class log:

    def __init__(self):
        self.filename = ""
        self.dict = {}

    def setLogFromFilename(self, filename):
        #input file should have format '(key1, value) (key2, value) (key3, value) ...'
        
        #set filename
        self.filename = str(filename)

        #open file and prepare for read
        file_read = open(str(filename), 'r')

        #parse file and add word to dict
        for line in file_read:
            pairs = line.split(" ")
            for pair in pairs:
                if str(pair) != '':
                    pair = str(pair).replace("(", '')
                    pair = pair.replace(")", '')
                    keyvalue = pair.split(",")
                    key = keyvalue[0]
                    value = keyvalue[1]
                    self.dict[key] = value

    def setLogFromString(self, string):
        #input string should be in the format 'filename word1 count1 word2 count 2...'

        #split input string into array
        input_string = string.split(" ")

        #set filename
        self.filename = input_string[0]
        
        i = 1
        while i < len(input_string):
            key = input_string[i]
            value = input_string[i+1]
            self.dict[key] = value
            i = i + 2
        

    def toString(self):
        mString = str(self.filename) + " "
        for key, value in self.dict.items():
            mString = mString + str(key) + " " + str(value) + " "
        return mString
        
    
        
