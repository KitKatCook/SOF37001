import sys
import os

sys.path.append(f"{os.getcwd()}/MBServer")
sys.path.append(f"{os.getcwd()}/MBClient")
sys.path.append(f"{os.getcwd()}/MBCommon")

from Zookeeper import Zookeeper

def main():
    Zookeeper()
    
if __name__ == "__main__":
    main()