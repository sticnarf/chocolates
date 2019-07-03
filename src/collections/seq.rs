use core::ops::Index;

pub trait IterableOnce {
    type Item;
    
}

pub trait Seq: Index<usize> {
    type Item;

    fn len(&self) -> usize;
    fn contains(&self, item: &Self::Item) -> bool where Self::Item: PartialEq;
    fn count(&self, f: impl FnMut(&Self::Item) -> bool) -> usize;
    fn empty(&self) -> bool;
    fn exists(&self, f: impl FnMut(&Self::Item) -> bool) -> bool;
    fn find(&self, f: impl FnMut(&Self::Item) -> bool) -> Option<&Self::Item>;
    fn find_last(&self, f: impl FnMut(&Self::Item) -> bool) -> Option<&Self::Item>;
    fn fold<I>(&self, init: I, f: impl FnMut(I, &Self::Item) -> I) -> I;
    fn for_all(&self, f: impl FnMut(&Self::Item) -> bool) -> bool;
    fn foreach(&self, f: impl FnMut(&Self::Item));
    fn head(&self) -> Option<&Self::Item>;
    fn last(&self) -> Option<&Self::Item>;
    fn index_where(&self, f: impl FnMut(&Self::Item) -> bool) -> usize;
    fn last_index_where(&self, f: impl FnMut(&Self::Item) -> bool) -> usize;
    fn index_of(&self, item: &Self::Item) -> usize where Self::Item: PartialEq;
    fn max(&self) -> Option<&Self::Item> where Self::Item: PartialOrd;
    fn min(&self) -> Option<&Self::Item> where Self::Item: PartialOrd;
    fn max_by(&self, key: impl FnMut(&Self::Item, &Self::Item) -> bool) -> Option<&Self::Item>;
    fn min_by(&self, key: impl FnMut(&Self::Item, &Self::Item) -> bool) -> Option<&Self::Item>;
}

impl<I> Seq for Vec<I> {
    type Item = I;

    #[inline]
    fn len(&self) -> usize {
        Vec::len(self)
    }

    #[inline]
    fn contains(&self, item: &I) -> bool where I: PartialEq {
        Vec::contains(self, item)
    }

    #[inline]
    fn count(&self, f: impl FnMut(&I) -> bool) -> usize {
        let mut c = 0;
        for i in self {
            if f(i) {
                c += 1;
            }
        }
        c
    }
}
