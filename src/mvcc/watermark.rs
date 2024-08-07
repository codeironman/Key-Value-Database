use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_default() += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let cnt = self.readers.get_mut(&ts).unwrap();
        *cnt -= 1;
        if *cnt == 0 {
            self.readers.remove(&ts);
        }
    }
    // 活跃时间戳的数量。
    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
    //最小时间戳
    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(ts, _)| *ts)
    }
}
