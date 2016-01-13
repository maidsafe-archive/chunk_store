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

use rustc_serialize::hex::{FromHex, ToHex};
use std::{cmp, env, error, fmt, fs};
use std::io::{self, Read, Write};
use std::path::Path;
use tempdir::TempDir;
use xor_name::{XorName, slice_as_u8_64_array};

const NOT_ENOUGH_SPACE_ERROR: &'static str = "Not enough storage space";
const CHUNK_NOT_FOUND_ERROR: &'static str = "Chunk not found";

#[allow(missing_docs)]
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    NotEnoughSpace,
    ChunkNotFound,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref error) => error.fmt(f),
            Error::NotEnoughSpace => write!(f, "{}", NOT_ENOUGH_SPACE_ERROR),
            Error::ChunkNotFound => write!(f, "{}", CHUNK_NOT_FOUND_ERROR),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref error) => error.description(),
            Error::NotEnoughSpace => NOT_ENOUGH_SPACE_ERROR,
            Error::ChunkNotFound => CHUNK_NOT_FOUND_ERROR,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Io(ref error) => Some(error),
            _ => None,
        }
    }
}

/// ChunkStore is a collection for holding all data chunks.
/// Implements a maximum disk usage to restrict storage.
///
/// The data chunks are deleted when the ChunkStore goes out of scope.
pub struct ChunkStore {
    tempdir: TempDir,
    max_space: usize,
    used_space: usize,
}

impl ChunkStore {
    /// Creates new ChunkStore with `max_space` allowed storage space.
    ///
    /// The data are stored in a temporary directory that contains `prefix`
    /// in its name and is placed in the `root` directory.
    /// If `root` doesn't exist, it will be created.
    pub fn new_in(root: &Path, prefix: &str, max_space: usize) -> Result<ChunkStore, Error> {
        fs::create_dir_all(root)
            .and_then(|()| TempDir::new_in(root, prefix))
            .map(|tempdir| {
                ChunkStore {
                    tempdir: tempdir,
                    max_space: max_space,
                    used_space: 0,
                }
            })
            .map_err(From::from)
    }

    /// Creates new ChunkStore with `max_space` allowed storage space.
    ///
    /// The data are stored in a temporary directory that contains `prefix`
    /// in its name and is placed in the system temp directory.
    pub fn new(prefix: &str, max_disk_usage: usize) -> Result<ChunkStore, Error> {
        Self::new_in(&env::temp_dir(), prefix, max_disk_usage)
    }

    /// Stores a new data chunk under `name`.
    ///
    /// If there is not enough storage space available,
    /// returns `Error::NotEnoughSpace`. In case of an IO error, it returns
    /// `Error::Io`.
    ///
    /// If the name already exists, it will be overwritten.
    pub fn put(&mut self, name: &XorName, value: &[u8]) -> Result<(), Error> {
        if !self.has_space(value.len()) {
            return Err(Error::NotEnoughSpace);
        }

        // If a file with name 'name' already exists, delete it.
        // We don't care if the delete fails here.
        let _ = self.delete(name);

        let hex_name = self.to_hex_string(name);
        let path_name = Path::new(&hex_name);
        let path = self.tempdir.path().join(path_name);

        fs::File::create(&path)
            .and_then(|mut file| {
                file.write_all(value)
                    .and_then(|()| file.sync_all())
                    .and_then(|()| file.metadata())
                    .map(|metadata| {
                        self.used_space += metadata.len() as usize;
                    })
            })
            .map_err(From::from)
    }

    /// Deletes the data chunk stored under `name`.
    ///
    /// If the name doesn't exist, it does nothing and returns `Ok`. In case
    /// of an IO error it returns `Error::Io`.
    pub fn delete(&mut self, name: &XorName) -> Result<(), Error> {
        if let Some(entry) = self.dir_entry(name) {
            if let Ok(metadata) = entry.metadata() {
                self.used_space -= cmp::min(metadata.len() as usize, self.used_space);
            }

            fs::remove_file(entry.path()).map_err(From::from)
        } else {
            Ok(())
        }
    }

    /// Reads a data chunk stored under `name`.
    ///
    /// If the name doesn't exist, returns `Error::ChunkNotFound`. In Case of
    /// an IO error, returns `Error::Io`.
    pub fn get(&self, name: &XorName) -> Result<Vec<u8>, Error> {
        let entry = self.dir_entry(name);
        if entry.is_none() {
            return Err(Error::ChunkNotFound);
        }

        let entry = entry.unwrap();
        let mut file = try!(fs::File::open(&entry.path()));

        let mut contents = Vec::<u8>::new();
        let _ = try!(file.read_to_end(&mut contents));

        Ok(contents)
    }

    /// Tests if a data chunk with `name` is stored in this ChunkStore.
    pub fn has_chunk(&self, name: &XorName) -> bool {
        self.dir_entry(name).is_some()
    }

    /// Lists names of all data chunks currently stored in this ChunkStore.
    pub fn names(&self) -> Vec<XorName> {
        fs::read_dir(&self.tempdir.path())
            .and_then(|dir_entries| {
                let dir_entry_to_routing_name = |dir_entry: io::Result<fs::DirEntry>| {
                    dir_entry.ok()
                             .and_then(|entry| entry.file_name().into_string().ok())
                             .and_then(|hex_name| hex_name.from_hex().ok())
                             .and_then(|bytes| Some(XorName::new(slice_as_u8_64_array(&*bytes))))
                };
                Ok(dir_entries.filter_map(dir_entry_to_routing_name).collect())
            })
            .unwrap_or(vec![])
    }

    /// Returns the maximum amount of storage space available for this ChunkStore.
    pub fn max_space(&self) -> usize {
        self.max_space
    }

    /// Returns the amount of storage space already used by this ChunkStore.
    pub fn used_space(&self) -> usize {
        self.used_space
    }

    /// Tests if there is enough storage space to store a data chunk of
    /// `required_space` bytes.
    pub fn has_space(&self, required_space: usize) -> bool {
        self.used_space + required_space <= self.max_space
    }

    fn to_hex_string(&self, name: &XorName) -> String {
        name.get_id().to_hex()
    }

    fn dir_entry(&self, name: &XorName) -> Option<fs::DirEntry> {
        fs::read_dir(self.tempdir.path()).ok().and_then(|mut entries| {
            let hex_name = self.to_hex_string(name);
            entries.find(|entry| {
                       match *entry {
                           Ok(ref entry) => entry.file_name().to_str() == Some(&hex_name),
                           Err(_) => false,
                       }
                   })
                   .and_then(|entry| entry.ok())
        })
    }
}
