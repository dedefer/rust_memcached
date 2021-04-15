use std::{
    slice, str, mem::take,
    collections::{HashMap, BTreeMap},
    time::{Instant, Duration},
    thread::sleep,
};
use log::debug;

struct Item {
    touch: Instant,
    ttl: Option<Instant>,
    data: Vec<u8>,
}

#[derive(Default)]
pub struct Memcached {
    limit: usize,
    current_size: usize,
    cache: HashMap<String, Item>,
    keys_by_ttl: BTreeMap<Instant, Vec<&'static str>>,
    keys_by_touch: BTreeMap<Instant, Vec<&'static str>>,
}

impl Memcached {
    pub fn new(limit: usize) -> Memcached {
        Memcached { limit, ..Default::default() }
    }

    pub fn delete(&mut self, key: &str) -> Option<Vec<u8>> {
        let (_key_owned, item) = self.cache.remove_entry(key)?;

        self.remove_from_touch(key, item.touch);
        self.remove_from_ttl(key, item.ttl);
        self.current_size -= item.data.len();

        Some(item.data)
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let item = self.cache.get(key)?;

        if let Some(ttl) = item.ttl {
            if ttl < Instant::now() {
                return None
            }
        }

        Some(item.data.clone())
    }

    pub fn set(&mut self, key: &str, value: &[u8], ttl: Option<Duration>) -> bool {
        let not_enough_space = |mc: &Self| (mc.current_size + value.len()) > mc.limit;

        if not_enough_space(self) {
            self.collect_garbage()
        }

        while not_enough_space(self) && self.remove_oldest() {
            debug!("oldest key displaced: current size {}", self.current_size);
        }

        if not_enough_space(self) {
            return false
        }

        self.delete(key);

        let touch = Instant::now();
        let ttl = ttl.map(|ttl| touch + ttl);

        let key_owned = key.to_owned();
        let key = unsafe { as_str_unsafe(&key_owned) };

        self.cache.insert(key_owned, Item {
            touch, ttl,
            data: value.to_owned(),
        });

        let mut new_keys_by_touch = self.keys_by_touch
            .remove(&touch).unwrap_or_else(|| Vec::with_capacity(1));
        new_keys_by_touch.push(key);
        self.keys_by_touch.insert(touch, new_keys_by_touch);


        if let Some(ttl) = ttl {
            let mut new_keys_by_ttl = self.keys_by_ttl
                .remove(&ttl).unwrap_or_else(|| Vec::with_capacity(1));
            new_keys_by_ttl.push(key);
            self.keys_by_ttl.insert(ttl, new_keys_by_ttl);
        }

        self.current_size += value.len();

        true
    }

    pub fn collect_garbage(&mut self) {
        let now = Instant::now();
        let keys_sets: Vec<(Instant, Vec<&str>)> = self.keys_by_ttl
            .iter_mut()
            .take_while(|(&ttl, _)| ttl < now)
            .map(|(&ttl, v)| { (ttl, take(v)) })
            .collect();

        let mut memory_retrieved = self.current_size;
        keys_sets.iter().for_each(|(ttl, keys)| keys.iter().for_each(|&key| {
            let (_key_owned, item) = self.cache.remove_entry(key).unwrap();

            self.remove_from_touch(key, item.touch);
            self.keys_by_ttl.remove(ttl);

            self.current_size -= item.data.len();
        }));

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
            keys.retain(|&k| k != key);
            if !keys.is_empty() {
                self.keys_by_ttl.insert(ttl, keys);
            }
        }
    }

    fn remove_from_touch(&mut self, key: &str, touch: Instant) {
        let mut keys = self.keys_by_touch.remove(&touch).unwrap();
        keys.retain(|&k| k != key);
        if !keys.is_empty() {
            self.keys_by_touch.insert(touch, keys);
        }
    }

    fn remove_oldest(&mut self) -> bool {
        let key = match self.keys_by_touch.iter().next() {
            Some((_, keys)) => keys.get(0).map(|&s| s)
                .expect("empty vec in keys_by_touch (impossibre)"),
            None => return false,
        };

        self.delete(&key).is_some()
    }
}


unsafe fn as_str_unsafe(s: &String) -> &'static str {
    str::from_utf8_unchecked(
        slice::from_raw_parts(s.as_ptr(), s.len())
    )
}


#[cfg(test)]
mod public_tests {
    use super::*;

    #[test]
    fn set_get_ok() {
        let mut mc = Memcached::new(300);
        mc.set("a", "a".as_bytes(), None);
        assert_eq!(mc.get("a"), Some("a".into()));
    }

    #[test]
    fn get_none() {
        let mc = Memcached::new(300);
        assert_eq!(mc.get("b"), None);
    }

    #[test]
    fn displace_oldest() {
        let mut mc = Memcached::new(3);
        mc.set("a", "a".as_bytes(), None);
        mc.set("b", "a".as_bytes(), None);
        mc.set("c", "a".as_bytes(), None);
        mc.set("d", "a".as_bytes(), None);

        assert_eq!(mc.get("a"), None);
        assert_eq!(mc.get("b"), Some("a".into()));
        assert_eq!(mc.get("c"), Some("a".into()));
        assert_eq!(mc.get("d"), Some("a".into()));
    }

    #[test]
    fn expire() {
        let mut mc = Memcached::new(300);
        mc.set("a", "a".as_bytes(), Some(Duration::from_millis(100)));
        assert_eq!(mc.get("a"), Some("a".into()));

        sleep(Duration::from_millis(200));

        assert_eq!(mc.get("a"), None);
    }

    #[test]
    fn overflow() {
        let mut mc = Memcached::new(1);
        mc.set("a", "aa".as_bytes(), None);
        assert_eq!(mc.get("a"), None);
    }
}


#[cfg(test)]
mod inner_tests {
    use super::*;

    /// test validates that pointers to keys are equal in cache, keys_by_touch and keys_by_ttl
    #[test]
    fn valid_pointers() {
        let mut mc = Memcached::new(300);
        mc.set("a", "a".as_bytes(), Some(Duration::from_secs(300)));

        let (key, v) = mc.cache.get_key_value("a").unwrap();
        let key_ttl = mc.keys_by_ttl[&v.ttl.unwrap()][0];
        let key_touch = mc.keys_by_touch[&v.touch][0];
        assert_eq!(key.as_ptr(), key_ttl.as_ptr());
        assert_eq!(key.as_ptr(), key_touch.as_ptr());
    }

    #[test]
    fn expire_without_gc() {
        let mut mc = Memcached::new(300);
        mc.set("a", "a".as_bytes(), Some(Duration::from_millis(100)));
        assert_eq!(mc.get("a"), Some("a".into()));

        sleep(Duration::from_millis(200));

        assert_eq!(mc.get("a"), None);
        assert_eq!(mc.current_size, 1);
        assert_eq!(mc.cache.len(), 1);
        assert_eq!(mc.keys_by_ttl.len(), 1);
        assert_eq!(mc.keys_by_touch.len(), 1);
    }

    #[test]
    fn expire_with_gc() {
        let mut mc = Memcached::new(300);
        mc.set("a", "a".as_bytes(), Some(Duration::from_millis(100)));
        assert_eq!(mc.get("a"), Some("a".into()));

        sleep(Duration::from_millis(200));
        mc.collect_garbage();

        assert_eq!(mc.get("a"), None);
        assert_eq!(mc.current_size, 0);
        assert_eq!(mc.cache.len(), 0);
        assert_eq!(mc.keys_by_ttl.len(), 0);
        assert_eq!(mc.keys_by_touch.len(), 0);
    }
}
