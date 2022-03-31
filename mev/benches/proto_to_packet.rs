#![feature(test)]

extern crate test;

use {
    solana_mev::{
        proto::packet::{Meta as PbMeta, Packet as PbPacket, PacketFlags as PbFlags},
        proto_packet_to_packet,
    },
    solana_sdk::packet::PACKET_DATA_SIZE,
    std::iter::repeat,
    test::Bencher,
};

#[bench]
fn bench_proto_to_packet(bencher: &mut Bencher) {
    let mut count = 0;
    bencher.iter(|| {
        let proto = PbPacket {
            data: repeat(count).take(PACKET_DATA_SIZE).collect(),
            meta: Some(PbMeta {
                size: PACKET_DATA_SIZE as u64,
                addr: "255.255.255.255:65535".to_string(),
                port: 65535,
                flags: Some(PbFlags {
                    discard: false,
                    forwarded: false,
                    repair: false,
                    simple_vote_tx: false,
                    tracer_tx: false,
                }),
            }),
        };
        proto_packet_to_packet(proto);

        count = (count + 1) % 255;
    });
}
