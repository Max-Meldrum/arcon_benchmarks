/// 
/// Powered by Lars Kroll
///

use criterion::{
    black_box,
    criterion_group,
    criterion_main,
    Bencher,
    BenchmarkId,
    Criterion,
    Throughput,
};

fn fibonacci_for_u(n: u64) -> u64 {
    assert!(n > 0u64);
    if n <= 2 {
        return 1;
    }
    let mut sum = 1;
    let mut last = 1;
    let mut cur = 1;
    for _ in 2..n {
        sum = last + cur;
        last = cur;
        cur = sum;
    }
    sum
}

fn fibonacci_for_s(n: i64) -> i64 {
    assert!(n > 0i64);
    if n <= 2 {
        return 1;
    }
    let mut sum = 1i64;
    let mut last = 1i64;
    let mut cur = 1i64;
    for _ in 2..n {
        sum = last + cur;
        last = cur;
        cur = sum;
    }
    sum
}

fn fibonacci_while_u(n: u64) -> u64 {
    assert!(n > 0u64);
    if n <= 2 {
        return 1;
    }
    let mut sum = 1;
    let mut last = 1;
    let mut cur = 1;
    let mut pos = 2u64;
    while pos < n {
        sum = last + cur;
        last = cur;
        cur = sum;
        pos += 1;
    }
    sum
}

fn fibonacci_while_s(n: i64) -> i64 {
    assert!(n > 0i64);
    if n <= 2 {
        return 1i64;
    }
    let mut sum = 1i64;
    let mut last = 1i64;
    let mut cur = 1i64;
    let mut pos = 2i64;
    while pos < n {
        sum = last + cur;
        last = cur;
        cur = sum;
        pos += 1i64;
    }
    sum
}

fn fibonacci_fold_u(n: u64) -> u64 {
    assert!(n > 0u64);
    if n <= 2 {
        return 1;
    }

    let (_, _, sum) = (2..n).fold((1, 1, 1),|acc, _v| {
        let (last, cur, _) = acc;
        let sum = last + cur;
        (cur, sum, sum)
    });
    sum
}

fn square_root_newton_for(square: u64, iters: usize) -> f64 {
    let target = square as f64;
    let mut current_guess = target;
    for _ in 0..iters {
        let numerator = current_guess * current_guess - target;
        let denom = current_guess * 2.0;
        current_guess = current_guess - (numerator/denom);
    }
    current_guess
}

fn square_root_newton_fold(square: u64, iters: usize) -> f64 {
    let target = square as f64;

    let result = std::iter::repeat(()).take(iters).fold(target, |current_guess, _| {
        let numerator = current_guess * current_guess - target;
        let denom = current_guess * 2.0;
        current_guess - (numerator/denom)
    });
    result
}

/*
* CRITERION
*/

pub fn mappers_fibonacci(c: &mut Criterion) {
    let mut group = c.benchmark_group("Fibonacci Mappers by Index");
    for index in [50, 80].iter() {
        group.bench_with_input(
            BenchmarkId::new("for unsigned",*index),
            index,
            benches::bench_fibonacci_for_u,
        );
        group.bench_with_input(
            BenchmarkId::new("while unsigned",*index),
            index,
            benches::bench_fibonacci_while_u,
        );
        group.bench_with_input(
            BenchmarkId::new("fold unsigned",*index),
            index,
            benches::bench_fibonacci_fold_u,
        );
        // group.bench_with_input(
        //     BenchmarkId::new("for signed",*index),
        //     index,
        //     benches::bench_fibonacci_for_s,
        // );
        // group.bench_with_input(
        //     BenchmarkId::new("while signed",*index),
        //     index,
        //     benches::bench_fibonacci_while_s,
        // );
    }
    group.finish();
}

pub fn mappers_newton(c: &mut Criterion) {
    let mut group = c.benchmark_group("Newton Mappers by Index");
    for index in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::new("for",*index),
            index,
            benches::bench_newton_for,
        );
        group.bench_with_input(
            BenchmarkId::new("fold",*index),
            index,
            benches::bench_newton_fold,
        );
        // group.bench_with_input(
        //     BenchmarkId::new("while unsigned",*index),
        //     index,
        //     benches::bench_fibonacci_while_u,
        // );
        // group.bench_with_input(
        //     BenchmarkId::new("fold unsigned",*index),
        //     index,
        //     benches::bench_fibonacci_fold_u,
        // );
        // group.bench_with_input(
        //     BenchmarkId::new("for signed",*index),
        //     index,
        //     benches::bench_fibonacci_for_s,
        // );
        // group.bench_with_input(
        //     BenchmarkId::new("while signed",*index),
        //     index,
        //     benches::bench_fibonacci_while_s,
        // );
    }
    group.finish();
}

criterion_group!(mappers_benches, mappers_fibonacci, mappers_newton);
criterion_main!(mappers_benches);

pub mod benches {
    use super::*;
    use criterion::Bencher;

    pub fn bench_fibonacci_for_u(b: &mut Bencher, index: &usize) {
        let fib = *index as u64;
        b.iter(|| fibonacci_for_u(fib));
    }
    pub fn bench_fibonacci_while_u(b: &mut Bencher, index: &usize) {
        let fib = *index as u64;
        b.iter(|| fibonacci_while_u(fib));
    }
    pub fn bench_fibonacci_fold_u(b: &mut Bencher, index: &usize) {
        let fib = *index as u64;
        b.iter(|| fibonacci_fold_u(fib));
    }
    // pub fn bench_fibonacci_for_s(b: &mut Bencher, index: &usize) {
    //     let fib = *index as i64;
    //     b.iter(|| fibonacci_for_s(fib));
    // }
    // pub fn bench_fibonacci_while_s(b: &mut Bencher, index: &usize) {
    //     let fib = *index as i64;
    //     b.iter(|| fibonacci_while_s(fib));
    // }

    pub fn bench_newton_for(b: &mut Bencher, index: &usize) {
        let root = *index as u64;
        let square = root * root;
        b.iter(|| square_root_newton_for(square, *index));
    }
    pub fn bench_newton_fold(b: &mut Bencher, index: &usize) {
        let root = *index as u64;
        let square = root * root;
        b.iter(|| square_root_newton_fold(square, *index));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIRST_N_FIB: [u64; 9] = [1, 1, 2, 3, 5, 8, 13, 21, 34];

    fn fibonacci_tests(f: fn(u64) -> u64) {
        for i in 0..FIRST_N_FIB.len() {
            let expected = FIRST_N_FIB[i];
            let index = i + 1; // don't do fib(0)!
            let result = f(index as u64);
            assert_eq!(expected, result);
        }
    }

    fn fibonacci_tests_signed(f: fn(i64) -> i64) {
        for i in 0..FIRST_N_FIB.len() {
            let expected = FIRST_N_FIB[i];
            let index = i + 1; // don't do fib(0)!
            let result = f(index as i64) as u64;
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn fibonacci_for_u_test() {
        fibonacci_tests(fibonacci_for_u);
    }

    #[test]
    fn fibonacci_while_u_test() {
        fibonacci_tests(fibonacci_while_u);
    }

    #[test]
    fn fibonacci_fold_u_test() {
        fibonacci_tests(fibonacci_fold_u);
    }

    #[test]
    fn fibonacci_for_s_test() {
        fibonacci_tests_signed(fibonacci_for_s);
    }

    #[test]
    fn fibonacci_while_s_test() {
        fibonacci_tests_signed(fibonacci_while_s);
    }

    const ACCURACY: f64 = 0.00000000001;

    fn sqrt_tests(f: fn(u64, usize) -> f64) {
        for i in 30..50 {
            let expected = i as f64;
            let root = i as u64;
            let square = root*root;
            let result = f(square, i);
            assert!((expected - result).abs() < ACCURACY);
        }
    }

    #[test]
    fn square_root_newton_for_test() {
        sqrt_tests(square_root_newton_for);
    }

    #[test]
    fn square_root_newton_fold_test() {
        sqrt_tests(square_root_newton_fold);
    }

}
