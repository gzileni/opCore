import json
import os

# get configuration product
def load():
    # read db parameters
    directory = os.getcwd()
    print(directory)
    f = open(directory + "/core/config.json")
    product_config = json.load(f)
    f.close()

    return product_config
