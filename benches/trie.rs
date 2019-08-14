use chocolates::collections::Trie;
use criterion::*;
use std::borrow::Cow;

fn insert_8_char_string(b: &mut Bencher) {
    let mut strings: Vec<_> = Vec::with_capacity(2000);
    for i in 1..1001 {
        strings.push(format!("{:x}", -i));
    }

    let mut m = Trie::with_capacity(2000);
    for key in &strings {
        m.insert(Cow::Borrowed(key.as_bytes()), key);
    }
    b.iter(|| {
        for key in &strings {
            m.insert(Cow::Borrowed(key.as_bytes()), key);
        }
    })
}

fn bench_trie(c: &mut Criterion) {
    c.bench_function("trie::insert_8_char_string", insert_8_char_string);
}

criterion_group!(benches, bench_trie);
criterion_main!(benches);
