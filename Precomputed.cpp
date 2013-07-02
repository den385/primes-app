#include "stdafx.h"

#include "Hashtable.h"

using namespace std;
using namespace stdext;

typedef long long int64;
typedef unsigned long long uint64;

const string path = "precomputed_bin/";
const int NUMBERS_IN_CHUNK = 10 * 1000 * 1000;
const int NCHUNKS32 = 20;
const int NCHUNKS64 = 5;
const int BYTES_IN_NUMBER32 = 4;
const int BYTES_IN_NUMBER64 = 8;
const int TOTAL_NUMBERS32 = NUMBERS_IN_CHUNK * NCHUNKS32;
const int TOTAL_NUMBERS64 = NUMBERS_IN_CHUNK * NCHUNKS64;
const unsigned long MIN32 = 0x00000000;
const unsigned long MAX32 = 0xFFFFFFFF;
const unsigned long long MAX64 = 5336500537;

static char * buffer32;
static char * buffer64;

/* Returns the amount of milliseconds elapsed since the UNIX epoch. Works on both
 * windows and linux. */
int64 GetTimeHns64()
{
 /* Windows */
 FILETIME ft;
 LARGE_INTEGER li;

 /* Get the amount of 100 nano seconds intervals elapsed since January 1, 1601 (UTC) and copy it
  * to a LARGE_INTEGER structure. */
 GetSystemTimeAsFileTime(&ft);
 li.LowPart = ft.dwLowDateTime;
 li.HighPart = ft.dwHighDateTime;

 uint64 ret = li.QuadPart;
 ret -= 116444736000000000LL; /* Convert from file time to UNIX epoch time. */
 
 return ret;
}

pair<unsigned long long, unsigned long long> Init( bool silent, Callback ReportProgress )
{
  if ( sizeof ( unsigned long ) != 4 )
		throw( "Bit size of unsigned long unexpected." );
	if ( sizeof ( unsigned long long ) != 8 )
		throw( "Bit size of unsigned long long unexpected." );

	//auto mytime = GetTimeHns64();	
	buffer32 = new char[ NCHUNKS32 * BYTES_IN_NUMBER32 * NUMBERS_IN_CHUNK ];
	buffer64 = new char[ NCHUNKS64 * BYTES_IN_NUMBER64 * NUMBERS_IN_CHUNK ];

	// process 32bit chunks of primes
	for ( int n=1; n <= NCHUNKS32; ++n ) 
	{
		ifstream* file = new ifstream( path + "precomputed" + to_string( (long long)n ), ios::in | ios::binary ); 
		if ( !file->is_open() )
			throw runtime_error("couldn't open");

		int shift32 = (n-1) * BYTES_IN_NUMBER32 * NUMBERS_IN_CHUNK;
		file->read( buffer32 + shift32, BYTES_IN_NUMBER32 * NUMBERS_IN_CHUNK );
		file->close();
		file->clear();
		delete file;

		if ( !silent )
			ReportProgress( n * 4 );
	}

	// process 64bit chunks of primes
	for ( int n=NCHUNKS32 + 1; n <= NCHUNKS32 + NCHUNKS64; ++n ) 
	{
		ifstream* file = new ifstream( path + "precomputed" + to_string( (long long) n ), ios::in | ios::binary ); 
		if ( !file->is_open() )
			throw runtime_error("couldn't open");

		int shift64 = (n-NCHUNKS32-1) * BYTES_IN_NUMBER64 * NUMBERS_IN_CHUNK;
		file->read( buffer64 + shift64, BYTES_IN_NUMBER64 * NUMBERS_IN_CHUNK );
		file->close();
		file->clear();
		delete file;

		if ( !silent )
			ReportProgress( n * 4 );
	}	

	return pair<unsigned long long, unsigned long long>( MIN32, MAX64 );
}

bool Request( unsigned long long a )
{
	unsigned long * decoded32 = reinterpret_cast<unsigned long *>( buffer32 );
	unsigned long long * decoded64 = reinterpret_cast<unsigned long long *>( buffer64 );
	bool result;

	if ( a <= MAX32 )
		result = binary_search( decoded32, decoded32 + TOTAL_NUMBERS32, (unsigned long)a );
	else
		result = binary_search( decoded64, decoded64 + TOTAL_NUMBERS64, a );

	return result;
}

void Destroy()
{
	delete[] buffer32;
	delete[] buffer64;
}
