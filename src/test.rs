// Copyright 2015 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under (1) the MaidSafe.net Commercial License,
// version 1.0 or later, or (2) The General Public License (GPL), version 3, depending on which
// licence you accepted on initial access to the Software (the "Licences").
//
// By contributing code to the SAFE Network Software, or to this project generally, you agree to be
// bound by the terms of the MaidSafe Contributor Agreement, version 1.0.  This, along with the
// Licenses can be found in the root directory of this project at LICENSE, COPYING and CONTRIBUTOR.
//
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//
// Please review the Licences for the specific language governing permissions and limitations
// relating to use of the SAFE Network Software.

macro_rules! assert_err {
    ($cond : expr, $error : pat) => {
        match $cond {
            Err($error) => (),
            result => panic!(concat!("Expecting ", stringify!($error), " got {:?}"), result),
        }
    }
}

#[cfg(test)]
mod test {
    use chunk_store::{ChunkStore, Error};
    use rand;
    use std::fs;
    use std::path::Path;
    use tempdir::TempDir;
    use xor_name::XorName;

    fn generate_random_bytes(size: usize) -> Vec<u8> {
        use rand::Rng;
        rand::thread_rng().gen_iter().take(size).collect()
    }

    fn is_dir_empty(dir: &Path) -> bool {
        if let Ok(mut entries) = fs::read_dir(dir) {
            !entries.next().is_some()
        } else {
            true
        }
    }

    #[test]
    fn tempdir_cleanup() {
        let root = unwrap_result!(TempDir::new("test"));

        {
            let _store = ChunkStore::new_in(root.path(), "test", 64);
            assert!(!is_dir_empty(root.path()));
        }

        assert!(is_dir_empty(root.path()));
    }

    #[test]
    fn successful_put() {
        let k_disk_size: usize = 116;
        let mut chunk_store = unwrap_result!(ChunkStore::new("test", k_disk_size));
        let mut names = vec![];

        {
            let mut put = |size| {
                let name = rand::random();
                let data = generate_random_bytes(size);
                let size_before_insert = chunk_store.used_space();
                assert!(!chunk_store.has_chunk(&name));
                unwrap_result!(chunk_store.put(&name, &data));
                assert_eq!(chunk_store.used_space(), size + size_before_insert);
                assert!(chunk_store.has_chunk(&name));
                names.push(name);
                chunk_store.used_space()
            };

            assert_eq!(put(1usize), 1usize);
            assert_eq!(put(100usize), 101usize);
            assert_eq!(put(10usize), 111usize);
            assert_eq!(put(5usize), k_disk_size);
        }

        assert_eq!(names.sort(), chunk_store.names().sort());
    }

    #[test]
    fn failed_put_when_not_enough_space() {
        let k_disk_size = 32;
        let mut store = unwrap_result!(ChunkStore::new("test", k_disk_size));
        let name = rand::random();
        let data = generate_random_bytes(k_disk_size + 1);

        assert_err!(store.put(&name, &data), Error::NotEnoughSpace);
    }

    #[test]
    fn delete() {
        let k_size: usize = 1;
        let k_disk_size: usize = 116;
        let mut chunk_store = unwrap_result!(ChunkStore::new("test", k_disk_size));

        let mut put_and_delete = |size| {
            let name = rand::random();
            let data = generate_random_bytes(size);

            unwrap_result!(chunk_store.put(&name, &data));
            assert_eq!(chunk_store.used_space(), size);
            unwrap_result!(chunk_store.delete(&name));
            assert_eq!(chunk_store.used_space(), 0);
        };

        put_and_delete(k_size);
        put_and_delete(k_disk_size);
    }

    #[test]
    fn put_and_get_value_should_be_same() {
        let data_size = 50;
        let k_disk_size: usize = 116;
        let mut chunk_store = unwrap_result!(ChunkStore::new("test", k_disk_size));

        let name = rand::random();
        let data = generate_random_bytes(data_size);
        unwrap_result!(chunk_store.put(&name, &data));
        let recovered = unwrap_result!(chunk_store.get(&name));
        assert_eq!(data, recovered);
        assert_eq!(chunk_store.used_space(), data_size);
    }

    #[test]
    fn repeatedly_storing_same_name() {
        let k_disk_size: usize = 116;
        let mut chunk_store = unwrap_result!(ChunkStore::new("test", k_disk_size));

        let mut put = |name, size| {
            let data = generate_random_bytes(size);
            unwrap_result!(chunk_store.put(&name, &data));
            chunk_store.used_space()
        };

        let name = rand::random::<XorName>();
        assert_eq!(put(name.clone(), 1usize), 1usize);
        assert_eq!(put(name.clone(), 100usize), 100usize);
        assert_eq!(put(name.clone(), 10usize), 10usize);
        assert_eq!(put(name.clone(), 5usize), 5usize);  // last inserted data size
    }

    #[test]
    fn get_fails_when_name_does_not_exist() {
        let chunk_store = unwrap_result!(ChunkStore::new("test", 64));
        let name = rand::random();

        assert_err!(chunk_store.get(&name), Error::ChunkNotFound);
    }
}
