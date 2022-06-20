import sys
import os

sys.path.append(f"{os.getcwd()}/MBServer")
sys.path.append(f"{os.getcwd()}/MBClient")
sys.path.append(f"{os.getcwd()}/MBCommon")

from Producer import Producer

def main():
    print("Producer starting...")
    Producer()

if __name__ == "__main__":
    main()