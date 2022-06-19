import sys
import os

from Producer import Producer

sys.path.append(f"{os.getcwd()}/MBServer")
sys.path.append(f"{os.getcwd()}/MBClient")
sys.path.append(f"{os.getcwd()}/MBCommon")

def main():
    print("Client starting...")
    Producer()

if __name__ == "__main__":
    main()
