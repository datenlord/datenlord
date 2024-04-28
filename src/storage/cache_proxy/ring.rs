//! The hash ring data structure

use core::fmt;
use std::cmp;
use std::hash::BuildHasher;
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use tracing::warn;

use crate::async_fuse::util::usize_to_u64;

/// The default slot size
/// We use u64::MAX as the default slot size
/// And we do not need to worry about the overflow
const DEFAULT_SLOT_SIZE: u64 = std::u64::MAX;

/// A trait for types that support clone, and print
pub trait NodeType: Clone + PartialEq + Hash + Eq {}

// impl<T> NodeType for T where T: Clone + std::fmt::Debug {}

/// A slot definition in the hash ring
#[derive(Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Slot<T>
where
    T: NodeType,
{
    /// The start offset of the slot
    start: u64,
    /// The end offset of the slot
    end: u64,
    /// The slot data, contains mapping info
    inner: T,
}

impl<T> Slot<T>
where
    T: NodeType,
{
    /// Create a new slot
    pub fn new(start: u64, end: u64, inner: T) -> Self {
        Self { start, end, inner }
    }

    /// Get the start offset of the slot
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Get the end offset of the slot
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Get the slot data
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: fmt::Debug> fmt::Debug for Slot<T>
where
    T: NodeType,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Slot: start: {}, end: {}, data: {:?}",
            self.start, self.end, self.inner
        )
    }
}

impl<T> PartialEq for Slot<T>
where
    T: NodeType,
{
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end
    }
}

impl<T> Eq for Slot<T> where T: NodeType {}

impl<T> Ord for Slot<T>
where
    T: NodeType,
{
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.start.cmp(&other.start)
    }
}

impl<T> PartialOrd for Slot<T>
where
    T: NodeType,
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.start.cmp(&other.start))
    }
}

/// The default hash builder
#[derive(Debug, Clone)]
pub struct DefaultHashBuilder;

impl BuildHasher for DefaultHashBuilder {
    type Hasher = DefaultHasher;

    fn build_hasher(&self) -> Self::Hasher {
        DefaultHasher::new()
    }
}

impl Default for DefaultHashBuilder {
    fn default() -> Self {
        DefaultHashBuilder
    }
}

/// The hash ring data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Ring<T, S = DefaultHashBuilder>
where
    T: NodeType,
    S: BuildHasher + Clone,
{
    /// The hash builder
    #[serde(skip)]
    hash_builder: S,
    /// The slots
    slots: Vec<Slot<T>>,
    /// T to slot id mapping, accelerate finding the slot
    /// The slot step of the ring
    capacity: u64,
    /// The version of the ring
    version: u64,
}

impl<T> Default for Ring<T>
where
    T: NodeType,
{
    fn default() -> Self {
        Ring {
            hash_builder: DefaultHashBuilder,
            slots: Vec::new(),
            capacity: DEFAULT_SLOT_SIZE,
            version: 0,
        }
    }
}

impl<T, S> Ring<T, S>
where
    T: NodeType,
    S: BuildHasher + Clone,
{
    /// Create a new hash ring with a given hash builder and capacity
    pub fn new(hash_builder: S) -> Self {
        Self {
            hash_builder,
            slots: Vec::new(),
            capacity: DEFAULT_SLOT_SIZE,
            version: 0,
        }
    }

    /// Create a new hash ring with current given hash builder and capacity
    pub fn dump(&self) -> Self {
        Self {
            // Use the default hash builder
            hash_builder: self.hash_builder.clone(),
            slots: self.slots.clone(),
            capacity: self.capacity,
            version: self.version,
        }
    }

    /// Update the ring with a given ring
    /// It will node update the hash builder
    pub fn update(&mut self, ring: Ring<T, S>) {
        self.slots = ring.slots.clone();
        self.capacity = ring.capacity;
        self.version = ring.version;
    }

    /// Get the slot length
    pub fn len_slots(&self) -> usize {
        self.slots.len()
    }

    /// Get the slot at a given index
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Get version
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Check if the ring is empty
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Clear the ring
    pub fn slots_clear(&mut self) {
        self.slots.clear();
    }
}

impl<T, S: BuildHasher> Ring<T, S>
where
    T: NodeType,
    S: BuildHasher + Clone,
{
    /// Add a node to a slot
    /// We will create a new slot and update slot mapping, then add to the ring
    /// If must is true, the ring need to be rebalanced or expanded
    pub fn add(&mut self, node: &T, must: bool) -> Option<T> {
        // If the ring is full, return None
        if usize_to_u64(self.slots.len()) >= self.capacity {
            return None;
        }

        // Try to modify the ring, so we need to increase the version
        // TODO1: if the version is too large, we need to reset it
        // if th slot allocation failed, we need to keep the version
        self.version += 1;

        // If there are no slots, add the first one covering the whole range
        if self.slots.is_empty() {
            // TODO:Try to use rc to avoid clone?
            let new_slot = Slot::new(1, self.capacity, node.clone());
            self.slots.push(new_slot);

            return Some(node.clone());
        }

        // 1-512 513-1024
        // 1-512 513-768 769-1024
        // 1-256 256-512 513-768 769-1024
        // Find the slot with the largest range
        let (index, _) = match self
            .slots
            .iter()
            .enumerate()
            .max_by_key(|&(_, slot)| slot.end - slot.start)
        {
            Some((index, slot)) => (index, slot.end - slot.start),
            None => return None,
        };

        // Calculate the new ranges for the split
        let slot_to_split = self.slots.get_mut(index)?;
        let mid_point = (slot_to_split.start + slot_to_split.end)
            .overflowing_div(2)
            .0;

        // Create new slot with the second half of the range
        let new_slot = Slot::new(mid_point + 1, slot_to_split.end, node.clone());

        // Update the end of the existing slot to the mid_point
        slot_to_split.end = mid_point;

        // Insert the new slot to index+1, and shift the rest of the slots
        self.slots.insert(index + 1, new_slot);

        // Try to rebalance the ring
        // If must is true and the rebalance failed, return None
        if must && !self.rebalance() {
            warn!("Rebalance failed");

            return None;
        }

        Some(node.clone())
    }

    /// Add a batch of slots
    /// If must is true, the ring need to be rebalanced or expanded
    pub fn batch_add(&mut self, nodes: Vec<T>, must: bool) -> Option<Vec<T>> {
        // Store the success nodes
        let mut success_nodes = Vec::new();

        // If must is true, we need to expand the ring
        if must {
            if !self.rebalance() {
                warn!("Rebalance failed");

                return None;
            }
        } else if usize_to_u64(self.slots.len() + nodes.len()) > self.capacity {
            // If not satisfy the expand condition but too many nodes, return None
            return None;
        } else {
            // Try to modify the ring, so we need to increase the version
            // TODO1: if the version is too large, we need to reset it
            // if th slot allocation failed, we need to keep the version

            // Iterate the nodes to add
            for node in nodes {
                // Try to rebalance it later
                if let Some(n) = self.add(&node.clone(), false) {
                    success_nodes.push(n);
                }
            }
        }

        // Try to rebalance the ring
        if must {
            self.rebalance();
        }

        Some(success_nodes)
    }

    /// Remove a slot
    /// If must is true, the ring need to be rebalanced
    pub fn remove(&mut self, node: T, must: bool) -> Option<T> {
        // Find the slot to remove
        // TODO: Find the slot with faster way?
        // If the slot is not found, return None
        let index = self.slots.iter().position(|slot| slot.inner == node)?;

        // Remove the slot by index
        let _removed = self.remove_by_index(index, false)?;

        // Try to rebalance the ring
        if must {
            self.rebalance();
        }

        Some(node)
    }

    /// Remove a slot by index
    pub fn remove_by_index(&mut self, index: usize, must: bool) -> Option<T> {
        // Try to modify the ring, so we need to increase the version
        // TODO1: if the version is too large, we need to reset it
        // TODO2: if the slot allocation failed, we need to keep the version
        // Maybe we need to get the atomic function to update this version
        self.version += 1;

        // If the slot is not found, return None
        if index >= self.slots.len() {
            return None;
        }

        // Remove the slot, shift the rest of the slots
        let removed_slot = self.slots.remove(index);

        if self.slots.is_empty() {
            return None;
        }

        // Merge current slot range to previous slot
        if index > 0 {
            // other slots
            let prev_slot = self.slots.get_mut(index - 1)?;
            prev_slot.end = removed_slot.end;
        } else if !self.slots.is_empty() {
            // first slot, try to merge to the next slot
            let first_slot = self.slots.get_mut(0)?;
            first_slot.start = removed_slot.start;
        } else {
            // no slot left
            return None;
        }

        // Try to rebalance the ring
        if must && !self.rebalance() {
            warn!("Rebalance failed");

            return None;
        }

        Some(removed_slot.inner)
    }

    /// Remove a batch of slots
    /// If must is true, the ring need to be rebalanced or expanded
    pub fn batch_remove(&mut self, nodes: &[T], must: bool) -> Option<Vec<T>> {
        // TODO: Find the slot with faster way?
        let mut indexes_to_remove: Vec<usize> = nodes
            .iter()
            .filter_map(|node| self.slots.iter().position(|slot| &slot.inner == node))
            .collect();

        // Try to modify the ring, so we need to increase the version
        indexes_to_remove.sort_unstable_by(|a, b| b.cmp(a));

        let mut success_nodes = Vec::new();

        // If must is true, we need to expand the ring
        // Find the slot to remove
        for index in indexes_to_remove {
            if let Some(n) = self.remove_by_index(index, false) {
                success_nodes.push(n);
            }
        }

        // Try to rebalance the ring
        if must && !self.rebalance() {
            warn!("Rebalance failed");

            return None;
        }

        Some(success_nodes)
    }

    /// Get the slot of a given key
    pub fn get_slot<U: Hash>(&self, key: &U) -> Option<&Slot<T>> {
        if self.slots.is_empty() {
            return None;
        }

        // let idx = get_hash(&self.hash_builder, key) % self.capacity;
        let idx = get_hash(&self.hash_builder, key);

        // Find the slot with binary search
        match self.slots.binary_search_by(|slot| slot.start.cmp(&idx)) {
            Err(index) => {
                if index == 0 {
                    // redirect to the last slot(ring)
                    self.slots.last()
                } else {
                    // previous start index
                    self.slots.get(index - 1)
                }
            }
            Ok(index) => self.slots.get(index),
        }
    }

    /// Get the node of a given key
    pub fn get_node<U: Hash>(&self, key: &U) -> Option<&T> {
        self.get_slot(key).map(Slot::inner)
    }

    /// Rebalance the ring
    /// Try to rebalance the ring
    pub fn rebalance(&mut self) -> bool {
        if self.slots.is_empty() {
            return false;
        }

        // update version
        self.version += 1;

        // calculate new slot size
        let total_range = self.capacity;
        let new_slot_size = total_range
            .overflowing_div(usize_to_u64(self.slots.len()))
            .0;
        let mut start = 1_u64;

        // update slot range
        for slot in &mut self.slots {
            slot.start = start;
            start += new_slot_size;
            slot.end = start - 1;
        }

        // update the last slot
        if let Some(last_slot) = self.slots.last_mut() {
            last_slot.end = self.capacity;
        }

        true
    }
}

/// Get the hash index of a key
fn get_hash<T, S>(hash_builder: &S, key: T) -> u64
where
    T: Hash,
    S: BuildHasher,
{
    hash_builder.hash_one(key)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct Node {
        id: u64,
    }

    impl NodeType for Node {}

    #[test]
    fn test_slot() {
        let node = Node { id: 1 };
        let slot = Slot::new(1, 10, node);

        assert_eq!(slot.start(), 1);
        assert_eq!(slot.end(), 10);
        assert_eq!(slot.inner().id, 1);
    }

    #[test]
    fn test_ring() {
        let node1 = Node { id: 1 };
        let node2 = Node { id: 2 };
        let node3 = Node { id: 3 };

        let mut ring = Ring::new(DefaultHashBuilder);

        ring.add(&node1.clone(), false);
        ring.add(&node2.clone(), false);
        ring.add(&node3.clone(), false);

        assert_eq!(ring.len_slots(), 3);
        assert_eq!(ring.capacity(), 1024);
        assert_eq!(ring.version(), 3);

        ring.remove(node2.clone(), false);

        assert_eq!(ring.len_slots(), 2);
        assert_eq!(ring.capacity(), 1024);
        assert_eq!(ring.version(), 4);
    }

    #[test]
    fn test_batch_add() {
        let node1 = Node { id: 1 };
        let node2 = Node { id: 2 };
        let node3 = Node { id: 3 };

        let mut ring = Ring::new(DefaultHashBuilder);

        ring.batch_add(vec![node1.clone(), node2.clone(), node3.clone()], false);

        assert_eq!(ring.len_slots(), 3);
        assert_eq!(ring.capacity(), 1024);
        assert_eq!(ring.version(), 3);

        // node1
        assert_eq!(ring.get_slot(&1_i32).unwrap().start, 1);
        assert_eq!(ring.get_slot(&1_i32).unwrap().end, 512);
        // node2
        assert_eq!(ring.get_slot(&999_i32).unwrap().start, 769);
        assert_eq!(ring.get_slot(&999_i32).unwrap().end, 1024);
        // node3
        assert_eq!(ring.get_slot(&10_000_i32).unwrap().start, 513);
        assert_eq!(ring.get_slot(&10_000_i32).unwrap().end, 768);

        ring.slots_clear();
        assert_eq!(ring.version(), 3);

        ring.batch_add(vec![node1.clone(), node2.clone(), node3.clone()], true);

        assert_eq!(ring.len_slots(), 3);
        assert_eq!(ring.capacity(), 1024);
        assert_eq!(ring.version(), 7); // 3 + 3 + 1(rebalance)

        // node1
        assert_eq!(ring.get_slot(&1_i32).unwrap().start, 1);
        assert_eq!(ring.get_slot(&1_i32).unwrap().end, 341);
        // node2
        assert_eq!(ring.get_slot(&999_i32).unwrap().start, 683);
        assert_eq!(ring.get_slot(&999_i32).unwrap().end, 1024);
        // node3
        assert_eq!(ring.get_slot(&10_000_i32).unwrap().start, 342);
        assert_eq!(ring.get_slot(&10_000_i32).unwrap().end, 682);
    }

    #[test]
    fn test_batch_remove() {
        let node1 = Node { id: 1 };
        let node2 = Node { id: 2 };
        let node3 = Node { id: 3 };

        let mut ring = Ring::new(DefaultHashBuilder);

        ring.batch_add(vec![node1.clone(), node2.clone(), node3.clone()], true);

        assert_eq!(ring.len_slots(), 3);
        assert_eq!(ring.capacity(), 1024);
        assert_eq!(ring.version(), 4); // 3 + 1

        ring.batch_remove(&[node2.clone()], true);

        assert_eq!(ring.len_slots(), 2);
        assert_eq!(ring.capacity(), 1024);
        assert_eq!(ring.version(), 6); // 4 + 1 + 1(rebalance)

        assert_eq!(ring.get_slot(&1_i32).unwrap().start, 1);
        assert_eq!(ring.get_slot(&1_i32).unwrap().end, 512);
        assert_eq!(ring.get_slot(&10_000_i32).unwrap().start, 513);
        assert_eq!(ring.get_slot(&10_000_i32).unwrap().end, 1024);

        ring.batch_remove(&[node3.clone()], true);

        assert_eq!(ring.len_slots(), 1);
        assert_eq!(ring.capacity(), 1024);
        assert_eq!(ring.version(), 8); // 6 + 1 + 1(rebalance)

        assert_eq!(ring.get_slot(&1_i32).unwrap().start, 1);
        assert_eq!(ring.get_slot(&1_i32).unwrap().end, 1024);
    }

    /// Test the ring add and remove
    ///
    ///
    /// 3 nodes without balance => [500617, 249146, 250236]
    /// ```
    /// let mut node_hit_count = vec![0; 3];
    /// for i in 1..1000000 {
    ///    let slot = ring.get_slot(&i).unwrap();
    ///        node_hit_count[slot.inner().id as usize - 1] += 1;
    /// }
    /// println!("{:?}", node_hit_count);
    /// ```
    #[test]
    fn test_ring_get_slot() {
        let node1 = Node { id: 1 };
        let node2 = Node { id: 2 };
        let node3 = Node { id: 3 };

        let mut ring = Ring::new(DefaultHashBuilder);

        ring.add(&node1, false);
        ring.add(&node2, false);
        ring.add(&node3, false);

        let slot = ring.get_slot(&1_i32).unwrap();
        assert_eq!(slot.inner().id, 1);

        let slot = ring.get_slot(&999_i32).unwrap();
        assert_eq!(slot.inner().id, 3);

        let slot = ring.get_slot(&10_000_i32).unwrap();
        assert_eq!(slot.inner().id, 2);
    }
}
