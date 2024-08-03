use bytes::{Buf, BufMut, Bytes};

pub const U16SIZE: usize = std::mem::size_of::<u16>();
pub struct Block {
    pub data: Vec<u8>,
    pub offsets: Vec<u16>,
}
///. block
///
impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(offsets_len as u16);
        buf.into()
    }
    pub fn decode(data: &[u8]) -> Self {
        let entry_offsets_len = (&data[data.len() - U16SIZE..]).get_u16() as usize;
        let data_end = data.len() - U16SIZE - entry_offsets_len * U16SIZE;
        let offsets_raw = &data[data_end..data.len() - U16SIZE];
        let offsets = offsets_raw
            .chunks(U16SIZE)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
