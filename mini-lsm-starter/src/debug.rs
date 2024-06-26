use crate::lsm_storage::{LsmStorageInner, MiniLsm};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        if !snapshot.imm_memtables.is_empty() {
            println!(
                "imm ({}): {:?}",
                snapshot.imm_memtables.len(),
                snapshot
                    .imm_memtables
                    .iter()
                    .map(|x| x.id())
                    .collect::<Vec<_>>(),
            );
        }
        if !snapshot.l0_sstables.is_empty() {
            println!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
/* better dump */

// use crate::lsm_storage::{LsmStorageInner, MiniLsm};

// impl LsmStorageInner {
//     pub fn dump_structure(&self) {
//         let snapshot = self.state.read();
//         if !snapshot.memtable.is_empty() {
//             println!(
//                 "mem ({}): (id, len){:?}",
//                 snapshot.memtable.len(),
//                 (snapshot.memtable.id(), snapshot.memtable.len()),
//             );
//         }
//         if !snapshot.imm_memtables.is_empty() {
//             println!(
//                 "imm ({}): {:?}",
//                 snapshot.imm_memtables.len(),
//                 snapshot
//                     .imm_memtables
//                     .iter()
//                     .map(|x| (x.id(), x.len()))
//                     .collect::<Vec<_>>(),
//             );
//         }
//         if !snapshot.l0_sstables.is_empty() {
//             println!(
//                 "L0 ({}): {:?}",
//                 snapshot.l0_sstables.len(),
//                 snapshot.l0_sstables,
//             );
//         }
//         for (level, files) in &snapshot.levels {
//             println!("L{level} ({}): {:?}", files.len(), files);
//         }
//     }
// }

// impl MiniLsm {
//     pub fn dump_structure(&self) {
//         self.inner.dump_structure()
//     }
// }
