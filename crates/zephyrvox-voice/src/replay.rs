/// The 128-packet sliding window used for server-to-client UDP sequences.
///
/// Voice media may arrive out of order, so the client accepts unseen packets
/// below the high-water mark while rejecting duplicates and packets that have
/// fallen outside the bounded window.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReplayWindow {
    high: u64,
    bits: [u64; 2],
}

impl ReplayWindow {
    /// Returns whether a sequence is new without changing the window.
    pub(crate) fn would_accept(&self, sequence: u64) -> bool {
        if sequence == 0 {
            return false;
        }
        if sequence > self.high {
            return true;
        }
        let offset = self.high - sequence;
        if offset >= 128 {
            return false;
        }
        let word = offset / 64;
        let bit = offset % 64;
        self.bits[word as usize] & (1_u64 << bit) == 0
    }

    /// Accepts a new sequence and advances the window when necessary.
    pub(crate) fn accept(&mut self, sequence: u64) -> bool {
        if !self.would_accept(sequence) {
            return false;
        }
        if sequence > self.high {
            let shift = sequence - self.high;
            if shift >= 128 {
                self.bits = [0, 0];
            } else {
                self.shift_left(shift as u32);
            }
            self.high = sequence;
            self.bits[0] |= 1;
            return true;
        }
        let offset = self.high - sequence;
        self.bits[(offset / 64) as usize] |= 1_u64 << (offset % 64);
        true
    }

    /// Shifts the bitmap when a new high-water sequence advances the window.
    fn shift_left(&mut self, shift: u32) {
        match shift {
            0 => {}
            1..64 => {
                self.bits[1] = (self.bits[1] << shift) | (self.bits[0] >> (64 - shift));
                self.bits[0] <<= shift;
            }
            64..128 => {
                self.bits[1] = self.bits[0] << (shift - 64);
                self.bits[0] = 0;
            }
            _ => unreachable!("large replay shifts are handled by accept"),
        }
    }
}
