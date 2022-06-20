import sys
import os

sys.path.append(f"{os.getcwd()}/MBServer")
sys.path.append(f"{os.getcwd()}/MBClient")
sys.path.append(f"{os.getcwd()}/MBCommon")

from Consumer import Consumer

def main():
    print("Consumer starting...")
    Consumer()

if __name__ == "__main__":
    main()