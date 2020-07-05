use std::ffi::OsStr;
use std::path::Path;

use clap::{App, Arg};

mod fuse;
mod memfs;

use memfs::MemoryFilesystem;

fn main() {
    env_logger::init();

    let matches = App::new("Fuse Low Level")
        .arg(Arg::with_name("mountpoint").required(true).index(1))
        .arg(
            Arg::with_name("options")
                .short('o')
                .value_name("OPTIONS")
                .about("Mount options")
                .multiple(true)
                .takes_value(true)
                .number_of_values(1),
        )
        .get_matches();

    let mountpoint = OsStr::new(matches.value_of("mountpoint").unwrap());
    let options: Vec<&OsStr> = match matches.values_of("options") {
        Some(options) => options
            .map(|o| o.split(','))
            .flatten()
            .map(|o| OsStr::new(o))
            .collect(),
        None => Vec::new(),
    };

    let fs = MemoryFilesystem::new(&mountpoint);
    fuse::mount(fs, Path::new(&mountpoint), &options)
        .unwrap_or_else(|_| panic!("Couldn't mount filesystem {:?}", mountpoint));
}

#[cfg(test)]
mod test {
    #[test]
    fn test_tmp() {
        fn u64fn(u64ref: u64) {
            dbg!(u64ref);
        }
        let num: u64 = 100;
        let u64ref = &num;
        u64fn(u64ref.clone());
    }

    #[test]
    fn test_skip() {
        let v = vec![1, 2, 3, 4];
        for e in v.iter().skip(5) {
            dbg!(e);
        }
    }

    #[test]
    fn test_vec() {
        let mut v = vec![1, 2, 3, 4, 5];
        let cap = v.capacity();
        v.truncate(3);
        assert_eq!(v.len(), 3);
        assert_eq!(v.capacity(), cap);

        let mut v2 = vec![0; 3];
        v.append(&mut v2);
        assert_eq!(v.len(), 6);
        assert!(v2.is_empty());
    }

    #[test]
    fn test_map_swap() {
        use std::collections::{btree_map::Entry, BTreeMap};
        use std::ptr;
        use std::sync::RwLock;
        let mut map = BTreeMap::<String, Vec<u8>>::new();
        let (k1, k2, k3, k4) = ("A", "B", "C", "D");
        map.insert(k1.to_string(), vec![1]);
        map.insert(k2.to_string(), vec![2, 2]);
        map.insert(k3.to_string(), vec![3, 3]);
        map.insert(k4.to_string(), vec![4, 4, 4, 4]);

        let lock = RwLock::new(map);
        let mut map = lock.write().unwrap();

        let e1 = map.get_mut(k1).unwrap() as *mut _;
        let e2 = map.get_mut(k2).unwrap() as *mut _;
        //std::mem::swap(e1, e2);
        unsafe {
            ptr::swap(e1, e2);
        }
        dbg!(&map[k1]);
        dbg!(&map[k2]);

        let e3 = map.get_mut(k3).unwrap();
        e3.push(3);
        dbg!(&map[k3]);

        let k5 = "E";
        let e = map.entry(k5.to_string());
        if let Entry::Vacant(v) = e {
            v.insert(vec![5, 5, 5, 5, 5]);
        }
        dbg!(&map[k5]);
    }
    #[test]
    fn test_map_entry() {
        use std::collections::BTreeMap;
        use std::mem;
        let mut m1 = BTreeMap::<String, Vec<u8>>::new();
        let mut m2 = BTreeMap::<String, Vec<u8>>::new();
        let (k1, k2, k3, k4, k5) = ("A", "B", "C", "D", "E");
        m1.insert(k1.to_string(), vec![1]);
        m1.insert(k2.to_string(), vec![2, 2]);
        m2.insert(k3.to_string(), vec![3, 3, 3]);
        m2.insert(k4.to_string(), vec![4, 4, 4, 4]);

        let e1 = &mut m1.entry(k1.to_string());
        let e2 = &mut m2.entry(k5.to_string());
        mem::swap(e1, e2);

        dbg!(m1);
        dbg!(m2);
    }
}
