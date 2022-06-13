import sys, os; 
sys.path.append(f"{os.getcwd()}/MBServer")
from Zookeeper.Zookeeper import Zookeeper

def main():
    Zookeeper()
    
if __name__ == "__main__":
    main()