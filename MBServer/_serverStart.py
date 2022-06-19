import sys
import os

sys.path.append(f"{os.getcwd()}/MBServer")
sys.path.append(f"{os.getcwd()}/MBClient")

from Zookeeper import Zookeeper

def main():
    Zookeeper()
    
if __name__ == "__main__":
    main()