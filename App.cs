using System;
using System.IO;
using System.Collections;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using System.Timers;
using BPSW;

using Timer = System.Timers.Timer;

namespace NivalApp
{
  public static class Error
  {
    public static string BadInputParams = "Bad input parameters.";
    public static string ArchitectureNotSupported = "Your computer architecture`s endianness is not supported.";
    public static string FileMissing = "File missing!";
    public static string FileBadOpen = "File opening error.";
    public static string InputCorrupted = "Input corrupted!";
  }
  public enum State
  {
    Preprocessing,
    Processing,
    Aggregating,
    Finalization
  }

  public static class FileStreamExtensions
  {
    // Allocate a 8 bit buffer for conversions between byte[6] input and UInt64 output.
    // Since endianness doesn`t change during the runtime, zeroing the array is redundant.
    private static byte[] array = new byte[8];

    public static UInt64 ReadSixBytes( this FileStream fStream, Int64 startPosition = -1 )
    {
      lock ( fStream )
      {
        long tmp = fStream.Position;
        if ( startPosition != -1 )
          fStream.Position = (Int64)startPosition;
        
        fStream.Read( array, 0, 6 ); // A0 A1 A2 A3 A4 A5 0 0

        if ( startPosition != -1 )
          fStream.Position = tmp;
      }

      return BitConverter.ToUInt64( array, 0 );
    }
    
    public static UInt64[] ReadSixByteNumbersChunk( this FileStream fStream, UInt64 numbersInChunk, Int64 startPosition = -1 )
    {
      var chunk = new UInt64[numbersInChunk];

      lock ( fStream )
      {
        Int64 tmp = fStream.Position;
        if ( startPosition != -1 )
          fStream.Position = (Int64)startPosition;

        for ( UInt64 i = 0; i < numbersInChunk; ++i )
        {          
          fStream.Read( array, 0, 6 ); // A0 A1 A2 A3 A4 A5 0 0
          chunk[i] = BitConverter.ToUInt64( array, 0 );
        }

        if ( startPosition != -1 )
          fStream.Position = tmp;
      }

      return chunk;
    }
  }
  public static class UInt64Extensions
  {
    // BPSW marshalling
    public static bool IsPrimeFast( this UInt64 number )
    {
      return BPSW.Wrapper.IsPrime( number );
    }

    // simple eratosthenos sieve for debugging
    public static bool IsPrimeSlow( this UInt64 number )
    {
      if ( number == 0 || number == 1 )
        return false;

      if ( number == 2 || number == 3 )
        return true;

      if ( ( number & 1 ) == 0 )
        return false;

      UInt64 x = (UInt64)Math.Sqrt( number ) + 1;
      for ( UInt64 i = 3; i < x; i += 2 )
        if ( ( number % i ) == 0 )
          return false;

      return true;
    }
  }
  public static class Logger
  {
    public static bool Mute { get; set; }

    private static DateTime last = DateTime.MinValue;

    public static void Init()
    {
      if ( last == DateTime.MinValue )
      {
        Console.WriteLine( "Program started: " + DateTime.Now );
        last = DateTime.Now;
      }

      Console.WriteLine( "Ctrl+C to abort with partial result." );
      Console.WriteLine();
    }

    public static void Click( string tag = "" )
    {
      if ( !Mute )
        Console.WriteLine( tag + ": " + ( DateTime.Now - last ).TotalMilliseconds + " ms" );

      last = DateTime.Now;
    }
    public static void Error( string e )
    {
      if ( !Mute )
      {
        Console.WriteLine( "Error:" );
        Console.WriteLine( e );
      }
    }

    public static void BeginPreprocessing()
    {
      if ( !Mute )
      {
        Console.WriteLine();
        Console.WriteLine( "Preparing data..." );
      }
    }
    public static void EndPreprocessing()
    {
      if ( !Mute )
      {
        Console.WriteLine( "Done." );
        Console.WriteLine();
      }
    }

    public static void BeginProcessing()
    {
      if ( !Mute )
      {
        Console.WriteLine();
        Console.WriteLine( "Processing data..." );
        Console.WriteLine( "0%............................................100%" );
      }
    }
    public static void EndProcessing()
    {
      if ( !Mute )
      {
        Console.Write( "\n" );
        Console.WriteLine( "Done." );
        Console.WriteLine();
        Console.WriteLine();
      }
    }

    public static void BeginInitHashtable()
    {
      if ( !Mute )
      {
        Console.WriteLine();
        Console.WriteLine( "Initializing precomputed data..." );
        Console.WriteLine( "0%............................................100%" );
      }
    }
    public static void EndInitHashtable()
    {
      if ( !Mute )
      {
        Console.Write( "\n" );
        Console.WriteLine( "Done." );
        Console.WriteLine();
      }
    }

    public static void BeginAggregation()
    {
      if ( !Mute )
        Console.WriteLine( "Aggregating result..." );
    }
    public static void EndAggregation()
    {
      if ( !Mute )
      {
        Console.WriteLine( "Done." );
        Console.WriteLine();
      }
    }

    public static void ReportDebug( PrimeSequencesManager[] managers, UInt64 nThreads )
    {
      if ( !Mute )
      {
        Console.WriteLine( "Threads result:" );
        for ( UInt64 i = 0; i < nThreads; ++i )
          Console.WriteLine( i + "\n" + managers[i].Best.ToString() + "\n\n" );
      }
    }
    public static void ReportResult( UInt64 nThreads, bool runNoHashtable, PrimeSequence result )
    {
      if ( !Mute )
      {
        Console.WriteLine( "\nThreads used: " + nThreads + "\n" + "Hashtable used: " + ( runNoHashtable ? "N" : "Y" ) );
        Click( "Time (ms)" );
        Console.WriteLine();
        Console.WriteLine();
      }

      // no mute
      Console.WriteLine( result.ToString() );
      Console.WriteLine();
      Console.WriteLine();
    }

    public static void Wait()
    {
      if ( !Mute )
        Console.ReadLine();
    }

    // no mute
    public static void Abort( State s )
    {
      Console.Write( "\n\n\n" );

      switch (s)
      {
        case State.Preprocessing: Console.WriteLine( "Preprocessing interrupted. Result is unavailable." ); break;
        case State.Processing: Console.WriteLine( "Processing interrupted. Result is partial." ); break;
        case State.Aggregating: Console.WriteLine( "Aggregation interrupted. Result is partial." ); break;
        case State.Finalization: Console.WriteLine( "Finalization interrupted. Result is full." ); break;
      }

      Console.WriteLine();
    }
  }

  public struct PrimeSequence
  {
    // First = first number added
    public UInt64 First;
    public UInt64 FirstOffset;
    public UInt64 Last;
    public UInt64 LastOffset;
    public UInt64 Length;

    public static PrimeSequence Zero()
    {
      var seq = new PrimeSequence()
      {
        First = 0,
        FirstOffset = 0,
        Last = 0,
        LastOffset = 0,
        Length = 0
      };

      return seq;
    }
    public static bool operator ==( PrimeSequence lhs, PrimeSequence rhs )
    {
      return
        ( lhs.First == rhs.First &&
        lhs.FirstOffset == rhs.FirstOffset &&
        lhs.Last == rhs.Last &&
        lhs.LastOffset == rhs.LastOffset &&
        lhs.Length == rhs.Length );
    }
    public static bool operator !=( PrimeSequence lhs, PrimeSequence rhs )
    {
      return
        ( lhs.First != rhs.First ||
        lhs.FirstOffset != rhs.FirstOffset ||
        lhs.Last != rhs.Last ||
        lhs.LastOffset != rhs.LastOffset ||
        lhs.Length != rhs.Length );
    }

    public bool Add( UInt64 newPrime, UInt64 newPrimeOffset )
    {
      // this is empty
      if ( Length == 0 )
      {
        Length = 1;
        First = newPrime;
        FirstOffset = newPrimeOffset;
        Last = newPrime;
        LastOffset = newPrimeOffset;        
        return true;
      }

      // newPrime doesn`t fit
      // for we already have some seq and new prime is either smaller than it`s end
      // or is equal but with other offset which guarantees us it`s not some duplicate
      if ( Length > 0 && ( newPrime < Last || ( this.Last == newPrime && this.LastOffset != newPrimeOffset ) ) )
        return false;

      if ( this.Last == newPrime && this.LastOffset == newPrimeOffset )
        throw ( new Exception( "Prime duplicated detected in one of prime sequences. Seq info:\n" + this.ToString() ) );

      // newPrime fits
      Length += 1 - (UInt64)( ( this.Last == newPrime && this.LastOffset == newPrimeOffset ) ? 1 : 0 );
      Last = newPrime;
      LastOffset = newPrimeOffset;
      return true;
    }    
    public bool Add( PrimeSequence tail )
    {
      // [this.First___this.Last] + [tail.First___tail.Last]
      // tail is empty or is copy of this
      // do not swallow this
      if ( tail.Length == 0 )
        return false;
      if ( tail == this )
        throw ( new Exception ( "PrimeSequence total duplicate detected error." ) );
      
      // this is empty
      if ( Length == 0 )
      {
        this = tail;
        return true;
      }

      // tail doesn`t fit
      if ( Length > 0 && ( tail.First < this.Last || ( this.Last == tail.First && this.LastOffset != tail.FirstOffset ) ) )
        return false;

      // tail fits
      Length += tail.Length - (UInt64)( ( this.Last == tail.First && this.LastOffset == tail.FirstOffset ) ? 1 : 0 );
      Last = tail.Last;
      LastOffset = tail.LastOffset;
      return true;
    }

    public bool IsEmpty() { return Length == 0; }
    public override string ToString()
    {
      var sb = new StringBuilder();

      sb.Append( "First: " ).Append( First ).Append( "\n" ).
        Append( "FirstOffset: " ).Append( FirstOffset ).Append( "\n" ).
        Append( "Last: " ).Append( Last ).Append( "\n" ).
        Append( "LastOffset: " ).Append( LastOffset ).Append( "\n" ).
        Append( "Length: " ).Append( Length );

      return sb.ToString();
    }
  }
  public class PrimeSequencesManager
  {
    private static readonly UInt64 bytesInNumber = 6;
    private static readonly UInt64 bytesBlockGranularity = 300;

    private static Func<UInt64, bool> hashtableCallback;
    private static UInt64 upper;
    private static UInt64 lower;
    private static FileStream fStream;

    private ManualResetEvent doneEvent;
    private UInt64 startByte;
    private UInt64 endByte;

    // for aggregating
    private PrimeSequence best;
    private PrimeSequence current;
    
    // for stitching
    private PrimeSequence head;
    private PrimeSequence tail;
    private bool headAndBest;
    private bool bestAndTail;
    private int bestIter;
    private int tailIter;

    // For progress bar
    private Double progress;
    private static Double progressDelta;

    public static PrimeSequence AggregateSequences( PrimeSequencesManager[] managers, UInt64 nThreads, bool stitch = true )
    {
      Logger.BeginAggregation();

      // [inner-tail][head-inner-tail][head-inner-tail][head-inner]
      // 1. Find best inner sequence - is it necessary?????????????????????????
      var seqAccumulator = new PrimeSequencesManager( null, 0, 0 );
      for ( UInt64 i = 0; i < nThreads; ++i )
        seqAccumulator.Add( managers[i].Best );
      //Console.WriteLine( seqAccumulator.Result );

      if ( stitch )
      {
        // 2. Find best stitched sequence

        // 2.1. Create list of stitchable sequences
        //      delimited with null sequences if not neighbours
        //      and with removed duplicates ( Head mb== Best mb== Tail )
        // might be like this:
        // [Head-seq-Best-seq-Tail][Head-Best-Tail][Head=Best-Tail][Head=Best=Tail][Head=Best-seq-Tail][Head-seq-Best=Tail]
        var pieces = new List<PrimeSequence>();
        for ( UInt64 i = 0; i < nThreads; ++i )
        {
          var m = managers[i];

          // 1. HEAD
          // if empty, it`s 1st piece, else [i].Tail is always neighbour to [i+1].Head
          pieces.Add( m.Head );

          // 2. BEST
          // Head-Best case
          if ( m.Best != m.Head && m.HeadAndBest )
            pieces.Add( m.Best );
          // Head-seq-Best case => add delimeter and, than, Best
          else if ( m.Best != m.Head && !m.HeadAndBest )
          {
            pieces.Add( PrimeSequence.Zero() );
            pieces.Add( m.Best );
          }
          // Head-seq-Best && Head=Best => Error
          else if ( m.Best == m.Head && m.HeadAndBest )
          {
            var errorText = "Stitching logic error occured in thread #" + i + ".";
            Console.WriteLine( errorText );
            throw ( new Exception( errorText ) );
          }
          // Head=Best case => ignore Best (duplicate)
          else { }

          // 3. TAIL
          // Best-Tail case
          if ( m.Tail != m.Best && m.BestAndTail )
            pieces.Add( m.Tail );
          // Best-seq-Tail case => add delimeter and, than, Tail
          else if ( m.Tail != m.Best && !m.BestAndTail )
          {
            pieces.Add( PrimeSequence.Zero() );
            pieces.Add( m.Tail );
          }
          // Best-seq-Tail && Best=Tail => Error
          else if ( m.Best == m.Head && m.HeadAndBest )
          {
            var errorText = "Stitching logic error occured in thread #" + i + ".";
            Console.WriteLine( errorText );
            throw ( new Exception( errorText ) );
          }
          // Best=Tail case => ignore Best (duplicate)
          else { }
        }

        // 2.2. Generate list of stitched sequences
        var stitchedList = new List<PrimeSequence>();
        var stitched = new PrimeSequence();
        for ( int i = 0; i < pieces.Count; ++i )
        {
          // if cannot stitch <=> doesn`t fit or is duplicate 
          // ( which is error ) or is delimiter, then
          // save this stitched seq and start new one
          if ( !stitched.Add( pieces[i] ) )
          {
            // save this seq
            stitchedList.Add( stitched );

            // start new seq with what we could not stitch
            // it`s ok if did not fit
            // it would`ve thrown ex if was duplicate
            // it`s ok if it`s delimiter since it would change nothing 
            // in blank new seq and would be rewritten
            stitched = new PrimeSequence();
            stitched = pieces[i];
          }
        }
        // we might have missed the last stitched
        if ( !stitchedList.Contains( stitched ) )
          stitchedList.Add( stitched );

        // 3. Find best sequence - either inner or stitched
        foreach ( var seq in stitchedList )
          seqAccumulator.Add( seq );
      }

      Logger.EndAggregation();

      return seqAccumulator.Best;
    }

    public PrimeSequencesManager( ManualResetEvent done, UInt64 start, UInt64 end ) 
    {
      startByte = start;
      endByte = end;      
      doneEvent = done;
      headAndBest = false;
      bestAndTail = false;
      bestIter = 0;
      tailIter = 0;
      progress = 0;
    }
    public void ThreadPoolCallback( Object threadContext )
    {
      var threadNumber = (UInt64)threadContext;

      // BLOCK % 6 == 0, CHUNK % 6 == 0 => TAIL % 6 == 0
      // BLOCK = [CHUNK|CHUNK|CHUNK|CHUNK|TAIL]
      UInt64 bytesInBlock = (UInt64)(endByte - startByte);
      UInt64 bytesInChunk = Math.Min( bytesInBlock, bytesBlockGranularity );
      UInt64 bytesInTail = bytesInBlock % bytesInChunk;
      UInt64 nInChunk = bytesInChunk / bytesInNumber;
      UInt64 nIterations = ( bytesInBlock / bytesInChunk ) + ( bytesInTail == 0 ? (UInt64)0 : (UInt64)1 );

      // Loop Chunk
      for ( UInt64 chunkIter = 0; chunkIter < nIterations; ++chunkIter )
      {
        // read chunk
        UInt64 bytesLeftInBlock = bytesInBlock - chunkIter * bytesInChunk;
        UInt64 nLeftInBlock = bytesLeftInBlock / bytesInNumber;
                            // blockPos(abs) + chunkPos(rel)
        UInt64 iterBytePos = startByte + chunkIter * bytesInChunk;
        UInt64 nInRequest = ( chunkIter == nIterations - 1 ? nLeftInBlock : nInChunk );
        var array = fStream.ReadSixByteNumbersChunk( nInRequest, (Int64)iterBytePos );

        // Loop Primes
        for ( UInt64 numberIter = 0; numberIter < nInRequest; ++numberIter )
        {
          var isPrime = false;
          var n = array[numberIter];
                                 // chunkPos(abs) + NumberPos(rel)
          var offset = (UInt64)( iterBytePos + numberIter * bytesInNumber );          

          if ( hashtableCallback != null && n >= lower && n <= upper )
            isPrime = hashtableCallback( n );
          else
            isPrime = n.IsPrimeFast();

          if ( isPrime )
            Add( n, offset );

          progress += progressDelta;                    
        }
      }

      doneEvent.Set();
    }

    public void Add( PrimeSequence newSeq, bool tryAppendToCurrent = false )
    {
      if ( tryAppendToCurrent && current.Add( newSeq ) )
        return;

      // if not, compare current with best,
      // refresh some flags and create new current
      UpdateBest();

      // new current starts with unaccepted number
      current.Add( newSeq );
    }
    public void Add( UInt64 number, UInt64 offset )
    {
      // try add to current
      if ( !current.Add( number, offset ) )
      {
        // if not, compare current with best,
        // refresh some flags and create new current
        UpdateBest();

        // new current starts with unaccepted number
        current.Add( number, offset );
      }
    }

    private void UpdateBest()
    {
      // There is no current => it was redundant call
      if ( current.IsEmpty() )
        return;

      // There is no best => init everything with current
      if ( best.IsEmpty() )
      {
        // best and tail changed together for 1st time - they`re initialized
        bestIter = 1;
        tailIter = 1;

        // we have just one seq for now => it`s head=best=last case
        // initially best is empty => we come here => tail and head will never be empty afterwards
        head = current;
        tail = current;
        best = current;

        // false so that duplicates are ignored
        headAndBest = false;
        bestAndTail = false;

        // we processed current seq and are starting new one
        current = new PrimeSequence();
        return;
      }

      // current is worse than best
      if ( ( best.Length > current.Length ) ||
           ( best.Length == current.Length && best.First > current.First ) ||
           ( best.Length == current.Length && best.First == current.First && best.FirstOffset < current.FirstOffset ) )
      {
        // head is already stated once and for all
        // current is worse though it`s later than previous tail
        tail = current;
        ++tailIter;

        // best is after head only if best was updated exactly once since initializing
        headAndBest = ( bestIter == 2 ? true : false );
        // tail is after best only if tail has exactly one iter more than best so that 
        // it shifted exactly one time in "current is worse than best" CASE
        bestAndTail = ( tailIter == bestIter + 1 ? true : false );

        // start new current
        current = new PrimeSequence();
        return;
      }

      // current is better than best
      best = current;
      ++bestIter;

      tail = current;
      // we don`t just increment tailIter cause we might lose consistency (unlikely though)
      // now iters are synced
      tailIter = bestIter;

      // best is after head only if best was updated exactly once since initializing
      headAndBest = ( bestIter == 2 ? true : false );
      // tail is after best only if tail has exactly one iter more than best so that 
      // it shifted exactly one time in "current is worse than best" CASE
      // iters are synced => we never get true here, still it expresses the logics
      bestAndTail = ( tailIter == bestIter + 1 ? true : false ); ;

      // start new current
      current = new PrimeSequence();
    }

    public static void Create( FileStream fs, Func<UInt64, bool> callback, UInt64 up, UInt64 low )
    {
      fStream = fs;
      hashtableCallback = callback;
      upper = up;
      lower = low;

      // to calc progress
      var l = fStream.Length;
      UInt64 bytesInJob = (UInt64)( l - l % 6 );
      UInt64 nInJob = bytesInJob / bytesInNumber;

      // with each processed number we do (delta)% of processing job
      progressDelta = ( 100.0 / (double)nInJob );
    }
    public static void Destroy()
    {
      hashtableCallback = null;
      fStream = null;
    }

    // 1. Return longest chain of ascending primes.
    // 2. If multiple satisfy (1), return the one with biggest 1st element.
    // 3. If multiple satisfy (1) and (2), return the one with smallest 1st element offset.
    public PrimeSequence Best
    {
      get
      {
        UpdateBest();
        return best;
      }
    }
    public PrimeSequence Head
    {
      get
      {
        UpdateBest();
        return head;
      }
    }
    public PrimeSequence Tail
    {
      get
      {
        UpdateBest();
        return tail;
      }
    }
    public bool HeadAndBest { get { return headAndBest; } }
    public bool BestAndTail { get { return bestAndTail; } }

    // For progress bar, % of the
    public Double Progress { get { return progress; } }
  }
  
  public static class Program
  {
    private static readonly int SERIAL_UPPER_BOUND = 4096;
    private static readonly string silent = "silent";
    private static readonly string nohashtable = "nohashtable";

    // Input flags
    private static bool runSilent = false;    
    private static bool runNoHashtable = false;
    private static string fileName;
    
    // Thread sync and cancellation state
    private static Object hashtableLock = new Object();
    private static State programState = State.Preprocessing;

    // Main resources management
    private static FileStream fStream;
    private static UInt64 lowerHashtablePrime;
    private static UInt64 upperHashtablePrime;
    private static PrimeSequencesManager[] managers;
    private static UInt64 nThreads;
    private static PrimeSequence result = PrimeSequence.Zero();

    // Timing
    private static Timer progressTimer;
    private static Int32 progressShown;
    private static Int32 progressHashtableShown;

    // Debug methods
    private static void Debug()
    {
      fStream = new FileStream( fileName, FileMode.Open, FileAccess.Read, FileShare.Read );

      var primes = new List<UInt64>();
      var offsets = new List<UInt64>();
      FillPrimesList( primes, offsets );
      PrintPrimesList( primes, offsets );
    }
    private static void FillPrimesList( List<UInt64> primes, List<UInt64> offsets )
    {
      while ( fStream.Position < fStream.Length - 5 )
      {
        var offset = (UInt64)fStream.Position;
        var number = fStream.ReadSixBytes();
        if ( number.IsPrimeFast() )
        {
          primes.Add( number );
          offsets.Add( offset );
        }
      }
    }
    private static void PrintPrimesList( List<UInt64> primes, List<UInt64> offsets )
    {
      var unsorted = new UInt64[ primes.Count ];
      primes.CopyTo( unsorted, 0 );
      var unsortedOffsets = new UInt64[ primes.Count ];
      offsets.CopyTo( unsortedOffsets, 0 );

      primes.Sort();

      Console.WriteLine( primes.Count > 0 ? "Prime | Rank | Offset:" : "No primes found." );
      for ( int i=0; i<primes.Count; ++i )
      {
        var prime = unsorted[i];
        var offset = unsortedOffsets[i];
        var idx = primes.FindIndex( d => d == prime );
        Console.WriteLine( prime + " | " + idx + " | " + offset );
      }
      Console.WriteLine( primes.Count > 0 ? "Total primes N: " + primes.Count : "" );
    }    
    private static void ConvertPrimesToBinary32()
    {
      try
      {
        fStream = new FileStream( fileName, FileMode.Open );
      }
      catch ( Exception e )
      {
        if ( fStream == null )
          return;

        fStream.Close();
        Logger.Error( Error.FileBadOpen );
        return;
      }

      var sr = new StreamReader( fStream );
      string line;
      UInt32 number;
      var numbers = new List<UInt32>();

      while ( ( line = sr.ReadLine() ) != null )
      {
        var sNumbers = line.Split( new char[] { ' ', '\t' } );

        foreach ( var sNumber in sNumbers )
        {
          UInt32.TryParse( sNumber, out number );
          numbers.Add( number );
        }
      }

      fStream.Close();
      fStream = new FileStream( "bin_" + fileName, FileMode.OpenOrCreate );
      var bw = new BinaryWriter( fStream );

      foreach ( var n in numbers )
        bw.Write( (UInt32)n );

      bw.Close();
      fStream.Close();
      numbers.Clear();
    }
    private static void ConvertPrimesToBinary64()
    {
      try
      {
        fStream = new FileStream( fileName, FileMode.Open );
      }
      catch ( Exception e )
      {
        if ( fStream == null )
          return;

        fStream.Close();
        Console.WriteLine( e.ToString() );
        return;
      }

      var sr = new StreamReader( fStream );
      string line;
      UInt64 number;
      var numbers = new List<UInt64>();

      while ( ( line = sr.ReadLine() ) != null )
      {
        var sNumbers = line.Split( new char[] { ' ', '\t' } );

        foreach ( var sNumber in sNumbers )
        {
          UInt64.TryParse( sNumber, out number );
          numbers.Add( number );
        }
      }

      fStream.Close();
      fStream = new FileStream( "bin_" + fileName, FileMode.OpenOrCreate );
      var bw = new BinaryWriter( fStream );

      foreach ( var n in numbers )
        bw.Write( (UInt64)n );

      bw.Close();
      fStream.Close();
      numbers.Clear();
    }
    private static void LoopRequestHashTable()
    {
      string line;
      UInt64 target;
      bool inputOk;
      bool result;
      do
      {
        line = Console.ReadLine();
        inputOk = UInt64.TryParse( line, out target );

        if ( !inputOk )
          continue;

        result = RequestHashTable( target );
        Console.WriteLine( result ? "Prime" : "Composite" );

      }
      while ( line != "exit" );
    }

    // Hashtable routines
    private static void InitHashTable()
    {
      Logger.BeginInitHashtable();
      BPSW.Wrapper.InitHashtable( ref lowerHashtablePrime, ref upperHashtablePrime, runSilent, OnProgressHashTable );
      Logger.EndInitHashtable();
    }
    private static bool RequestHashTable( UInt64 target )
    {
      lock ( hashtableLock )
      {
        return BPSW.Wrapper.RequestHashtable( target );
      }
    }
    private static void DestroyHashTable()
    {
      BPSW.Wrapper.DestroyHashtable();
    }

    // Timing and cancelling
    private static void OnProgressTimer( object source, ElapsedEventArgs e )
    {
      if ( managers == null )
        return;

      Double progress = 0;
      foreach ( var m in managers )
        progress += m.Progress;

      var newProgress = (Int32)Math.Round( progress );
      var oldNSymbols = progressShown / 2;
      var newNSymbols = newProgress / 2;
      var addNSymbols = newNSymbols - oldNSymbols;

      if ( addNSymbols > 0 )
      {
        var sb = new StringBuilder();
        sb.Append( '|', addNSymbols );
        Console.Write( sb.ToString() );
      }

      progressShown = newProgress;
    }
    private static void OnProgressHashTable( Double percentDone )
    {
      // Print progress
      Double progress = percentDone;

      var newProgress = (Int32)progress;
      var oldNSymbols = progressHashtableShown / 2;
      var newNSymbols = newProgress / 2;
      var addNSymbols = newNSymbols - oldNSymbols;

      if ( addNSymbols > 0 )
      {
        var sb = new StringBuilder();
        sb.Append( '|', addNSymbols );
        Console.Write( sb.ToString() );
      }

      progressHashtableShown = newProgress;
    }
    private static void OnConsoleCancel( object sender, ConsoleCancelEventArgs e )
    {
      Logger.Mute = true;
      Logger.Abort( programState );

      switch ( programState )
      {
        case State.Preprocessing : break;
        case State.Processing:
          result = PrimeSequencesManager.AggregateSequences( managers, nThreads, false );
          Logger.ReportResult( nThreads, runNoHashtable, result );
          break;
        case State.Aggregating: 
          result = PrimeSequencesManager.AggregateSequences( managers, nThreads, false );
          Logger.ReportResult( nThreads, runNoHashtable, result );
          break;
        case State.Finalization: Logger.ReportResult( nThreads, runNoHashtable, result ); break;
      }
      
      Destroy();
    }    

    // Main
    private static bool TryInitProgram( string[] args )
    {
      Console.CancelKeyPress += OnConsoleCancel;

      // args[0] = FileName - always; args1 and args2 may be silent and nohashtable modifiers
      if ( args.Length == 0 || args[0] == null || args.Intersect<string>( new string[] { silent, nohashtable } ).Count() + 1 != args.Length )
      {
        Logger.Error( Error.BadInputParams );
        return false;
      }

      if ( !BitConverter.IsLittleEndian )
      {
        Logger.Error( Error.ArchitectureNotSupported );
        return false;
      }

      fileName = args[0];
      if ( !File.Exists( args[0] ) )
      {
        Logger.Error( Error.FileMissing );
        return false;
      }

      runSilent = args.Count( s => s == silent ) == 1 ? true : false;
      Logger.Mute = runSilent;
      Logger.Init();

      runNoHashtable = args.Count( s => s == nohashtable ) == 1 ? true : false;
      if ( !runNoHashtable )
        InitHashTable();

      Func<UInt64, bool> hashtableCallback = null;
      if ( !runNoHashtable )
        hashtableCallback = RequestHashTable;

      try
      {
        fStream = new FileStream( fileName, FileMode.Open, FileAccess.Read, FileShare.Read );
      }
      catch ( Exception e )
      {
        fStream.Close();
        Logger.Error( Error.FileBadOpen );
        return false;
      }

      // heuristically and logically, best number of threads
      // equals number of logical processors
      nThreads = ( fStream.Length < SERIAL_UPPER_BOUND ) ? 1 : (UInt64)Environment.ProcessorCount;

      PrimeSequencesManager.Create( fStream, hashtableCallback, upperHashtablePrime, lowerHashtablePrime );

      if ( !runSilent )
      {
        progressTimer = new Timer( 100 );
        progressTimer.Elapsed += OnProgressTimer;
        progressShown = 0;
        progressHashtableShown = 0;
      }
      
      return true;
    }
    private static void Exec()
    {
      Logger.BeginPreprocessing();
      managers = new PrimeSequencesManager[nThreads];
      var doneEvents = new ManualResetEvent[nThreads];

      // [fStream] = [Chunk|Chunk|Chunk|Tail]
      // make: chunk % 6 = 0, file % 6 = 0
      UInt64 bytesInFile = (UInt64)fStream.Length - (UInt64)( fStream.Length % 6 );
      UInt64 bytesInChunk = bytesInFile / nThreads;
      bytesInChunk += 6 - bytesInChunk % 6;

      for ( UInt64 i = 0; i < nThreads; ++i )
      {
        // Illustration
        // 0             12     18   24  27
        // XXXXXX XXXXXX ...... ...... 000
        UInt64 firstUnassignedBytePos = (UInt64)(i * bytesInChunk);
        UInt64 begin = firstUnassignedBytePos;
        UInt64 bytesInFileLeft = bytesInFile - firstUnassignedBytePos;
        UInt64 bytesInAssignment = ( i == nThreads - 1 ? bytesInFileLeft : bytesInChunk );
        UInt64 end = begin + bytesInAssignment;

        doneEvents[i] = new ManualResetEvent( false );
        managers[i] = new PrimeSequencesManager( doneEvents[i], begin, end );        
      }

      for ( UInt64 i = 0; i < nThreads; ++i )
        ThreadPool.QueueUserWorkItem( managers[i].ThreadPoolCallback, i );
      Logger.EndPreprocessing();

      Logger.BeginProcessing();
      programState = State.Processing;
      if ( !runSilent )
        progressTimer.Start();

      WaitHandle.WaitAll( doneEvents );
      if ( !runSilent )
        OnProgressTimer( null, null );
      Logger.EndProcessing();

      if ( !runSilent )
      {
        progressTimer.Stop();
        progressTimer.Elapsed -= OnProgressTimer;
        progressTimer = null;
      }

      programState = State.Aggregating;

      result = PrimeSequencesManager.AggregateSequences( managers, nThreads );
      programState = State.Finalization;

      Logger.ReportResult( nThreads, runNoHashtable, result );
    }    
    private static void Destroy()
    {
      PrimeSequencesManager.Destroy();

      if ( fStream != null )
      {
        fStream.Close();
        fStream = null;
      }
      
      if ( !runNoHashtable )
        DestroyHashTable();

      if ( progressTimer != null )
        progressTimer.Elapsed -= OnProgressTimer;

      progressTimer = null;
    }
    private static void Main( string[] args )
    {
      try
      {
        if ( !TryInitProgram( args ) )
          return;

        Exec();
        
        Logger.Wait();          
      }
      finally
      {
        Destroy();
      }
    }
  }
}
