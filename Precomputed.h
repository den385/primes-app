// to measure time
#include <Windows.h>

#include <hash_set>
#include <cstdio>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <iostream>
#include <iterator>
#include <algorithm>

typedef void (*Callback)( double );

std::pair<unsigned long long, unsigned long long> Init( bool silent, Callback ReportProgress );
bool Request( unsigned long long a );
void Destroy();
