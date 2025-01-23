import sys

current_word = None
current_count = 0
for line in sys.stdin:
    word, count = line.strip().split('\t')
    count  = int(count)
    
    if current_word == word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
            #print("%05d\t%s" %(current_count, current_word))
        current_word = word
        current_count = count

if current_word is not None:
    print(f"{current_word}\t{current_count}")
    #print("%05d\t%s" %(current_count, current_word))
