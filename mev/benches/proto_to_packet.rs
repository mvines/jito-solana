#![feature(test)]

extern crate test;

use {
    solana_mev::{
        proto::packet::{
            Meta as PbMeta, Packet as PbPacket, PacketBatch, PacketBatchList,
            PacketFlags as PbFlags,
        },
        proto_packet_to_packet,
    },
    solana_sdk::packet::{Packet, PACKET_DATA_SIZE},
    std::iter::repeat,
    test::Bencher,
};

fn get_proto_packet(i: u8) -> PbPacket {
    PbPacket {
        data: repeat(i).take(PACKET_DATA_SIZE).collect(),
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
    }
}

#[bench]
fn bench_proto_to_packet(bencher: &mut Bencher) {
    let packet_batch_list = PacketBatchList {
        header: None,
        batch_list: (0..1000)
            .map(|_| PacketBatch {
                packets: (0..128).map(|i| get_proto_packet(i)).collect(),
            })
            .collect(),
    };
    bencher.iter(|| {
        // let packets: Vec<Vec<Packet>> = packet_batch_list
        //     .batch_list
        //     .iter()
        //     .map(|b| {
        //         b.packets
        //             .iter()
        //             .map(|p| proto_packet_to_packet(p.clone()))
        //             .collect()
        //     })
        //     .collect();
        let p = proto_packet_to_packet(get_proto_packet(1));
    });
}
