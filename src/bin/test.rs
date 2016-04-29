#![feature(question_mark)]
extern crate spanning_rope;
extern crate rand;

use rand::distributions::{IndependentSample, Range};

use spanning_rope::{SpanningRope, StatsReporter};

fn main() {
    let mut rope = SpanningRope::new(None, None);
    let (k, kprime, v, vprime) = ("abc".as_bytes(),
                                  "abd".as_bytes(),
                                  "123".as_bytes(),
                                  "124".as_bytes());
    let _ = rope.insert(k, v);
    let _ = rope.insert(kprime, vprime);
    assert_eq!(rope.get(k).unwrap(), Some(v));

    println!("key count: {:?}", rope.key_count());
    // assert_eq!(rope.get(k).unwrap(), Some(vprime));


    let mut num_rope: SpanningRope<u32, u32> = SpanningRope::new(None, None);

    let ranged = Range::new(0u32, 100u32);
    let mut rng = rand::thread_rng();
    for i in 1..100 {
        let a: u32 = ranged.ind_sample(&mut rng);
        let _ = num_rope.insert(a, i);
        println!("key count: {:?}", num_rope.key_count());
    }
}
