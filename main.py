from node import Node
import threading


def main():
    node1 = Node()
    node2 = Node()
    node3 = Node()

    threads = [
        threading.Thread(target=node1.start, daemon=True),
        threading.Thread(target=node2.start, daemon=True),
        threading.Thread(target=node3.start, daemon=True),
    ]

    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
