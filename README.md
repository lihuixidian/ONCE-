# ONCE(+)
We provide a one-pass solution to count distinct/non-overlapped frenquency of time-constrained episodes from a long sequence.

## Input:
a long sequence in the form of: [(s1,time1),(s2,time2),...]

episodes:(s1,s2,...), e.g., (2, 68, 65) or ('A','B','C').

time constraint: an interger number, it equals $t_n$-$t_1$, where the $t_1$ and $t_n$ are the timestamps for the first and last element in an episode, respectively.

## Output:
Depending on whether ONCE or ONCE+ are called, the code will output the non-overlapped or distinct frequency for the targeted time-constrained episode.

## Corresponding projects:
Another version for ONCE(+) optimized for Spark and Sparkstreaming can be found in https://github.com/lihuixidian/ONCESpark, which shall be released sooner.

## Citaion:
For detailed information for the algorithms, please check and cite the following paper:



## Other information:
For more information, please refer to the homepage for the corresponding author, Prof. Hui at http://lihuixidian.github.io.




