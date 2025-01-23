import sys

for line in sys.stdin:
    try:
        words = line.strip().split()
        for word in words:
            print(f"{word}\t1")
    except:
        continue
