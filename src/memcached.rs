use std::mem::take;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use log::{debug};

struct Item {
    touch: Instant,
    ttl: Option<Instant>,
    data: Vec<u8>,
}

pub struct Memcached {
    limit: usize,
    current_size: usize,
    cache: HashMap<String, Item>,
    keys_by_ttl: BTreeMap<Instant, Vec<String>>,
    keys_by_touch: BTreeMap<Instant, Vec<String>>,
}

impl Memcached {
    pub fn new(limit: usize) -> Memcached {
        Memcached {
            limit: limit,
            current_size: 0,
            cache: HashMap::new(),
            keys_by_ttl: BTreeMap::new(),
            keys_by_touch: BTreeMap::new(),
        }
    }

    pub fn delete(&mut self, key: &str) -> Option<()> {
        let item = match self.cache.remove(key) {
            Some(item) => item,
            None => return None,
        };

        self.remove_from_touch(key, item.touch);
        self.remove_from_ttl(key, item.ttl);
        self.current_size -= item.data.len();

        Some(())
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let item = match self.cache.get(key) {
            Some(item) => item,
            None => return None,
        };

        if let Some(ttl) = item.ttl {
            if ttl < Instant::now() {
                return None
            }
        }

        Some(item.data.clone())
    }

    pub fn set(&mut self, key: &str, value: &[u8], ttl: Option<Duration>) -> Option<()> {
        let not_enough_space = |mc: &Self| (mc.current_size + value.len()) > mc.limit;

        if not_enough_space(self) {
            self.collect_garbage()
        }

        while not_enough_space(self) && self.cache.len() != 0 {
            self.remove_oldest();
            debug!("oldest key displaced");
        }

        if not_enough_space(self) {
            return None
        }

        self.delete(key);

        let now = Instant::now();
        let ttl = ttl.map(|ttl| now + ttl);

        self.cache.insert(key.to_owned(), Item {
            touch: now,
            ttl: ttl,
            data: value.to_owned(),
        });

        let mut new_keys_by_touch = self.keys_by_touch
            .remove(&now).unwrap_or_else(|| Vec::with_capacity(1));
        new_keys_by_touch.push(key.to_owned());
        self.keys_by_touch.insert(now, new_keys_by_touch);


        if let Some(ttl) = ttl {
            let mut new_keys_by_ttl = self.keys_by_ttl
                .remove(&ttl).unwrap_or_else(|| Vec::with_capacity(1));
            new_keys_by_ttl.push(key.to_owned());
            self.keys_by_ttl.insert(ttl, new_keys_by_ttl);
        }

        self.current_size += value.len();

        Some(())
    }

    pub fn collect_garbage(&mut self) {
        let now = Instant::now();
        let keys_sets: Vec<_> = self.keys_by_ttl.iter_mut()
            .take_while(|(&ttl, _)| ttl < now)
            .map(|(&ttl, v)| { (ttl, take(v)) })
            .collect();

        let mut memory_retrieved = self.current_size;
        keys_sets.iter().for_each(|(_, keys)| keys.iter().for_each(|key| {
            let item = self.cache.remove(key).unwrap();

            self.remove_from_touch(key, item.touch);

            self.current_size -= item.data.len();
        }));


        self.keys_by_ttl = self.keys_by_ttl.iter_mut()
            .skip_while(|(&ttl, _)| ttl < now)
            .map(|(&ttl, v)| { (ttl, take(v)) })
            .collect();

        memory_retrieved -= self.current_size;
        if memory_retrieved != 0 {
            debug!("gc retrieved {}B in {:?}", memory_retrieved, now.elapsed());
        }
    }
}

impl Memcached {
    fn remove_from_ttl(&mut self, key: &str, ttl: Option<Instant>) {
        if let Some(ttl) = ttl {
            let mut keys = self.keys_by_ttl.remove(&ttl).unwrap();
            keys.retain(|k| k != key);
            if !keys.is_empty() {
                self.keys_by_ttl.insert(ttl, keys);
            }
        }
    }

    fn remove_from_touch(&mut self, key: &str, touch: Instant) {
        let mut keys = self.keys_by_touch.remove(&touch).unwrap();
        keys.retain(|k| k != key);
        if !keys.is_empty() {
            self.keys_by_touch.insert(touch, keys);
        }
    }

    fn remove_oldest(&mut self) {
        let key = match self.keys_by_touch.iter().next() {
            Some((_, keys)) => keys[0].clone(),
            None => return,
        };

        self.delete(&key);
    }
}
