use zephyrvox_voice::nonce_for;

#[test]
fn nonce_is_zero_prefixed_big_endian_sequence() {
    assert_eq!(
        nonce_for(0x0102_0304_0506_0708),
        [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]
    );
}
