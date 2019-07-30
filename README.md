# ONCE(+)
This project provide a solution to count frenquency of episodes with time constraint.

# Input:
series dataset.
Its format: [(s1,time1),(s2,time2),...]
episodes:(s1,s2,...), like (2, 68, 65) or ('A','B','C').
max time internal, an interger number, it equals $t_n$-$t_1$, where the $t_1$ and $t_n$ are the timestamps for the first and last element in an episode, respectively.

# Output
Depending on whether ONCE or ONCE+ are called, the code will output the non-overlapped or distinct frequency for the targeted time-constrained episode.

# Citaion





