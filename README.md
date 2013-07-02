MonotonousPrimeSequences
========================

Console App in C# and C++ to find the longest asc sequence of primes in binary file. Binary file is interpreted 
6byte-wise and the remainder is cut off.

The app utilizes multithreading, BPSW algorithm and precomputed plain array. This array is a sorted storage 
for small primes. Also there are several basic optimizations like bitwise shift to recognize even numbers and hardcoded
primes up till 1000.

The app draws has CLI with progress bar in pseudographics.

The app is written in C# and native C++ with  glue code in C++/CLI.

The algorithmic code is BPSW.cpp was borrowed from http://e-maxx.ru/algo/bpsw.

My sole purpose for writing this code was to practice basic C# multithreading and few other things.
