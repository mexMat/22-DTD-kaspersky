import sys
import json
from AssemplySystem import Assembly

if __name__ == "__main__":
    config_filename = sys.argv[1]
    with open(config_filename, "r") as f:
        config = json.load(f)

    print(Assembly(config).execute())
