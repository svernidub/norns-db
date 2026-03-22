#[derive(bincode::Encode, bincode::Decode)]
pub struct State {
    pub ss_table_block_size: usize,
    pub memtable_size: usize,
    pub level_0_ss_tables: usize,
    pub level_1_ss_tables: usize,
    pub level_0_size: usize,
}
