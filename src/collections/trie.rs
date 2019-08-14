use std::borrow::Cow;
use std::mem;

#[derive(Debug)]
pub struct Trie<V> {
    nodes: Vec<Node<V>>,
}

enum SearchResult {
    Append {
        node_pos: usize,
        key_pos: usize,
    },
    Split {
        node_pos: usize,
        key_pos: usize,
        node_key_pos: usize,
    },
    ReplaceEmpty {
        node_pos: usize,
    },
    ReplaceExist {
        node_pos: usize,
    },
    Insert {
        node_pos: usize,
        key_pos: usize,
        children_pos: usize,
    },
    Init,
}

impl<V> Trie<V> {
    #[inline]
    pub fn new() -> Trie<V> {
        Trie { nodes: vec![] }
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Trie<V> {
        Trie {
            nodes: Vec::with_capacity(cap),
        }
    }

    fn search(&self, key: &[u8]) -> SearchResult {
        let mut node_pos = 0;
        let mut key_pos = 0;
        'outer: loop {
            let node = &self.nodes[node_pos];
            let mut node_key_pos = 0;
            while key_pos < key.len()
                && node_key_pos < node.key.len()
                && key[key_pos] == node.key[node_key_pos]
            {
                key_pos += 1;
                node_key_pos += 1;
            }
            if key_pos < key.len() {
                if node_key_pos < node.key.len() {
                    return SearchResult::Split {
                        node_pos,
                        key_pos,
                        node_key_pos,
                    };
                } else if node.children.is_empty() {
                    return SearchResult::Append { node_pos, key_pos };
                } else if node.children.len() < 6 {
                    for (i, c) in node.children.iter().enumerate() {
                        if self.nodes[*c].key[0] == key[key_pos] {
                            node_pos = *c;
                            continue 'outer;
                        } else if self.nodes[*c].key[0] > key[key_pos] {
                            return SearchResult::Insert {
                                node_pos,
                                key_pos,
                                children_pos: i,
                            };
                        }
                    }
                    return SearchResult::Append { node_pos, key_pos };
                } else {
                    let (mut left, mut right) = (0, node.children.len());
                    while left < right {
                        let i = (left + right) / 2;
                        let n = &self.nodes[i];
                        if n.key[0] == key[key_pos] {
                            node_pos = i;
                            continue 'outer;
                        } else if n.key[0] < key[key_pos] {
                            left = i + 1;
                        } else {
                            right = i;
                        }
                    }
                    return SearchResult::Insert {
                        node_pos,
                        key_pos,
                        children_pos: left,
                    };
                }
            } else if node_key_pos < node.key.len() {
                return SearchResult::Split {
                    node_pos,
                    key_pos,
                    node_key_pos,
                };
            } else {
                return if node.value.is_none() {
                    SearchResult::ReplaceEmpty { node_pos }
                } else {
                    SearchResult::ReplaceExist { node_pos }
                };
            }
        }
    }

    pub fn entry<'a>(&'a mut self, key: Cow<'a, [u8]>) -> Entry<'a, V> {
        let search_result = if !self.nodes.is_empty() {
            match self.search(&key) {
                SearchResult::ReplaceExist { node_pos } => {
                    return Entry::Occupied(OccupiedEntry {
                        trie: self,
                        key,
                        node_pos,
                    });
                }
                r => r,
            }
        } else {
            SearchResult::Init
        };
        Entry::Vacant(VacantEntry {
            trie: self,
            key,
            search_result,
        })
    }

    pub fn insert(&mut self, key: Cow<[u8]>, value: V) -> Option<V> {
        match self.entry(key) {
            Entry::Occupied(mut o) => Some(o.insert(value)),
            Entry::Vacant(v) => {
                v.insert(value);
                None
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        if let Entry::Occupied(o) = self.entry(Cow::Borrowed(key)) {
            Some(o.remove())
        } else {
            None
        }
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        !self.nodes.is_empty()
            && match self.search(key) {
                SearchResult::ReplaceExist { .. } => true,
                _ => false,
            }
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        if self.nodes.is_empty() {
            None
        } else {
            match self.search(key) {
                SearchResult::ReplaceExist { node_pos } => self.nodes[node_pos].value.as_ref(),
                _ => None,
            }
        }
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        if self.nodes.is_empty() {
            None
        } else {
            match self.search(key) {
                SearchResult::ReplaceExist { node_pos } => self.nodes[node_pos].value.as_mut(),
                _ => None,
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[derive(Debug)]
struct Node<V> {
    value: Option<V>,
    key: Vec<u8>,
    parent: usize,
    children: Vec<usize>,
}

pub enum Entry<'a, V: 'a> {
    Occupied(OccupiedEntry<'a, V>),
    Vacant(VacantEntry<'a, V>),
}

impl<'a, V> Entry<'a, V> {
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(default),
        }
    }

    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(default()),
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Entry::Occupied(o) => o.key(),
            Entry::Vacant(v) => v.key(),
        }
    }

    pub fn and_modify<F: FnOnce(&mut V)>(mut self, f: F) -> Entry<'a, V> {
        if let Entry::Occupied(ref mut o) = self {
            f(o.get_mut());
        }
        self
    }
}

impl<'a, V: Default> Entry<'a, V> {
    pub fn or_default(self) -> &'a mut V {
        self.or_insert_with(V::default)
    }
}

pub struct OccupiedEntry<'a, V: 'a> {
    trie: &'a mut Trie<V>,
    key: Cow<'a, [u8]>,
    node_pos: usize,
}

impl<'a, V> OccupiedEntry<'a, V> {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    fn remove_value(&mut self) -> V {
        let (mut should_removed, value) = {
            let node = &mut self.trie.nodes[self.node_pos];
            (node.children.is_empty(), node.value.take().unwrap())
        };
        let mut pos = self.node_pos;
        while should_removed {
            let node = self.trie.nodes.swap_remove(pos);
            if pos != 0 {
                let len = self.trie.nodes.len();
                if pos != len {
                    for c in &self.trie.nodes[pos].children {
                        if *c != pos {
                            unsafe {
                                let n: &mut Node<V> =
                                    &mut *(self.trie.nodes.as_ptr().add(*c) as *mut _);
                                n.parent = pos;
                            }
                        }
                    }
                    let mut parent = self.trie.nodes[pos].parent;
                    if parent == len {
                        parent = pos;
                    }
                    if self.trie.nodes[parent].children.len() < 6 {
                        for c in &mut self.trie.nodes[parent].children {
                            if *c == len {
                                *c = pos;
                                break;
                            }
                        }
                    } else {
                        let c = self.trie.nodes[pos].key[0];
                        let index = self.trie.nodes[parent]
                            .children
                            .binary_search_by_key(&c, |pos| {
                                if *pos == len {
                                    c
                                } else {
                                    self.trie.nodes[*pos].key[0]
                                }
                            })
                            .unwrap();
                        self.trie.nodes[parent].children[index] = pos;
                    }
                }
                let parent = if node.parent == len { pos } else { node.parent };
                let child_pos = {
                    let parent_node = &self.trie.nodes[parent];
                    if parent_node.children.len() < 6 {
                        parent_node.children.iter().position(|c| *c == pos).unwrap()
                    } else {
                        let c = node.key[0];
                        parent_node
                            .children
                            .binary_search_by_key(&c, |p| {
                                if *p == pos {
                                    c
                                } else {
                                    self.trie.nodes[*p].key[0]
                                }
                            })
                            .unwrap()
                    }
                };
                let parent_node = &mut self.trie.nodes[parent];
                parent_node.children.remove(child_pos);
                should_removed = parent_node.children.is_empty();
                pos = parent;
            } else {
                break;
            }
        }
        value
    }

    pub fn remove(mut self) -> V {
        self.remove_value()
    }

    pub fn get(&self) -> &V {
        self.trie.nodes[self.node_pos].value.as_ref().unwrap()
    }

    pub fn get_mut(&mut self) -> &mut V {
        self.trie.nodes[self.node_pos].value.as_mut().unwrap()
    }

    pub fn into_mut(self) -> &'a mut V {
        self.trie.nodes[self.node_pos].value.as_mut().unwrap()
    }

    pub fn insert(&mut self, value: V) -> V {
        self.trie.nodes[self.node_pos].value.replace(value).unwrap()
    }

    pub fn remove_entry(mut self) -> (Vec<u8>, V) {
        let v = self.remove_value();
        (self.key.into_owned(), v)
    }
}

pub struct VacantEntry<'a, V: 'a> {
    trie: &'a mut Trie<V>,
    key: Cow<'a, [u8]>,
    search_result: SearchResult,
}

impl<'a, V> VacantEntry<'a, V> {
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn into_key(self) -> Vec<u8> {
        self.key.into_owned()
    }

    pub fn insert(self, value: V) -> &'a mut V {
        let len = self.trie.nodes.len();
        let new_pos = match self.search_result {
            SearchResult::Append { node_pos, key_pos } => {
                self.trie.nodes.push(Node {
                    value: Some(value),
                    parent: node_pos,
                    key: self.key[key_pos..].to_vec(),
                    children: vec![],
                });
                self.trie.nodes[node_pos].children.push(len);
                len
            }
            SearchResult::Insert {
                node_pos,
                key_pos,
                children_pos,
            } => {
                self.trie.nodes.push(Node {
                    value: Some(value),
                    parent: node_pos,
                    key: self.key[key_pos..].to_vec(),
                    children: vec![],
                });
                self.trie.nodes[node_pos].children.insert(children_pos, len);
                len
            }
            SearchResult::ReplaceEmpty { node_pos } => {
                self.trie.nodes[node_pos].value.replace(value);
                node_pos
            }
            SearchResult::Split {
                node_pos,
                key_pos,
                node_key_pos,
            } => {
                let n = if key_pos < self.key.len() {
                    let nn = Node {
                        value: Some(value),
                        parent: node_pos,
                        key: self.key[key_pos..].to_vec(),
                        children: vec![],
                    };
                    let children =
                        if self.key[key_pos] < self.trie.nodes[node_pos].key[node_key_pos] {
                            vec![len, len + 1]
                        } else {
                            vec![len + 1, len]
                        };
                    self.trie.nodes.push(nn);
                    let n = &mut self.trie.nodes[node_pos];
                    Node {
                        value: n.value.take(),
                        parent: node_pos,
                        key: n.key.split_off(node_key_pos),
                        children: mem::replace(&mut n.children, children),
                    }
                } else {
                    let n = &mut self.trie.nodes[node_pos];
                    Node {
                        value: n.value.replace(value),
                        parent: node_pos,
                        key: n.key.split_off(node_key_pos),
                        children: mem::replace(&mut n.children, vec![len]),
                    }
                };
                let new_pos = self.trie.nodes.len();
                for c in &n.children {
                    self.trie.nodes[*c].parent = new_pos;
                }
                self.trie.nodes.push(n);
                len
            }
            SearchResult::Init => {
                self.trie.nodes.push(Node {
                    value: Some(value),
                    parent: 0,
                    key: self.key.into_owned(),
                    children: vec![],
                });
                0
            }
            SearchResult::ReplaceExist { .. } => unreachable!(),
        };
        self.trie.nodes[new_pos].value.as_mut().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::Trie;
    use std::borrow::Cow;

    #[test]
    fn test_trie() {
        let mut t = Trie::new();
        assert!(t.is_empty());
        let keys: Vec<&'static [u8]> = vec![b"bced", b"bcfd", b"bcad", b"deabc"];
        for (i, key) in keys.iter().enumerate() {
            t.insert(Cow::Borrowed(*key), i);
            assert_eq!(t.get(*key), Some(&i), "{:?}", t);
            assert!(!t.is_empty());
        }
        for (i, key) in keys.iter().enumerate() {
            assert!(!t.is_empty());
            assert_eq!(t.remove(*key), Some(i), "{:?} {:?}", t, key);
            assert_eq!(t.get(*key), None, "{:?} {:?}", t, key);
        }
        assert!(t.is_empty());
    }
}
