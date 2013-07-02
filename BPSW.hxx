#pragma once

#pragma unmanaged
#include <vector>

#pragma managed

namespace BPSW
{
  static const int primes[168] = { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71,
										  			 			 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173,
																	 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281,
																	 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409,
																	 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541,
																	 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659,
																	 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809,
																	 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941,
																	 947, 953, 967, 971, 977, 983, 991, 997 };

	void ReportProgress( double percent );

	public value class Wrapper
	{
	private:		
		
		//! Модуль 64-битного числа
		static long long abs (long long n);
		static unsigned long long abs (unsigned long long n);

		//! Возвращает true, если n четное
		template<class T>
		static bool even (const T & n);

		//! Делит число на 2
		template<class T>
		static void bisect (T & n);

		//! Умножает число на 2
		template<class T>
		static void redouble (T & n);

		//! Возвращает true, если n - точный квадрат простого числа
		template<class T>
		static bool perfect_square (const T & n);

		//! Вычисляет корень из числа, округляя его вниз
		template<class T>
		static T sq_root (const T & n);

		//! Возвращает количество бит в числе
		template<class T>
		static unsigned bits_in_number (T n);

		//! Возвращает значение k-го бита числа (биты нумеруются с нуля)
		template<class T>
		static bool test_bit (const T & n, unsigned k);

		//! Умножает a *= b (mod n)
		template<class T>
		static void mulmod (T & a, T b, const T & n);

		//! Вычисляет a^k (mod n)
		template<class T, class T2>
		static T powmod (T a, T2 k, const T & n);

		//! Переводит число n в форму q*2^p
		template<class T>
		static void transform_num (T n, T & p, T & q);

		//! Алгоритм Евклида
		template<class T, class T2>
		static T gcd (const T & a, const T2 & b);

		//! Вычисляет jacobi(a,b) - символ Якоби
		template<class T>
		static T jacobi (T a, T b);

		//! Вычисляет pi(b) первых простых чисел. Возвращает вектор с простыми и в pi - pi(b)
		template<class T, class T2>
		static const std::vector<T> & get_primes (const T & b, T2 & pi);

		//! Тривиальная проверка n на простоту, перебираются все делители до m.
		//! Результат: 1 - если n точно простое, p - его найденный делитель, 0 - если неизвестно
		template<class T, class T2>
		static T2 prime_div_trivial (const T & n, T2 m);

		//! Алгоритм Бэйли-Померанс-Селфридж-Вагстафф (BPSW) проверки n на простоту
		template<class T>
		static bool isprime (T n);
		
	public:

		static System::Action<System::Double>^ hashtableProgressCallback;

		static bool IsPrime( System::UInt64 value );

		static void InitHashtable( System::UInt64% lower, System::UInt64% upper, bool silent, System::Action<System::Double>^ callback );

		static bool Wrapper::RequestHashtable( System::UInt64 target );

		static void DestroyHashtable();
	};
}
