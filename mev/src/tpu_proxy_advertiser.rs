//! The `tpu_proxy_advertiser` listens on a notification
//! channel for connect/disconnect to validator interface
//! and advertises TPU proxy addresses when connected.

use {
    crossbeam_channel::{Receiver, RecvTimeoutError},
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    std::{
        net::SocketAddr,
        sync::Arc,
        thread::{self, JoinHandle},
        time::Duration,
    },
};

pub struct TpuProxyAdvertiser {
    thread_hdl: JoinHandle<()>,
}

impl TpuProxyAdvertiser {
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        tpu_notify_receiver: Receiver<Option<(SocketAddr, SocketAddr)>>,
        heartbeat_timeout: u64,
    ) -> TpuProxyAdvertiser {
        let cluster_info = cluster_info.clone();
        let thread_hdl = thread::spawn(move || {
            info!("[MEV] Started TPU proxy advertiser");
            let saved_contact_info = cluster_info.my_contact_info();
            Self::advertise(
                &cluster_info,
                saved_contact_info.tpu,
                saved_contact_info.tpu_forwards,
                tpu_notify_receiver,
                heartbeat_timeout,
            )
        });
        Self { thread_hdl }
    }

    fn advertise(
        cluster_info: &Arc<ClusterInfo>,
        saved_tpu: SocketAddr,
        saved_tpu_forwards: SocketAddr,
        mut tpu_notify_receiver: Receiver<Option<(SocketAddr, SocketAddr)>>,
        heartbeat_timeout: u64,
    ) {
        let mut is_advertising_proxy = false;

        loop {
            match tpu_notify_receiver.recv_timeout(Duration::from_millis(heartbeat_timeout)) {
                Ok(Some((tpu_address, tpu_forward_address))) => {
                    if !is_advertising_proxy {
                        info!("TPU proxy connected, advertising remote TPU ports");
                        Self::set_tpu_addresses(cluster_info, tpu_address, tpu_forward_address);
                        is_advertising_proxy = true;
                    }
                }
                // either tpu proxy detected heartbeat dead or receiving timed out
                // either way, we should turn back proxy
                Ok(None) | Err(RecvTimeoutError::Timeout) => {
                    if is_advertising_proxy {
                        info!("TPU proxy heartbeat died, advertising my TPU ports");
                        Self::set_tpu_addresses(cluster_info, saved_tpu, saved_tpu_forwards);
                        is_advertising_proxy = false;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if is_advertising_proxy {
                        info!("channel disconnected, exiting");
                        Self::set_tpu_addresses(cluster_info, saved_tpu, saved_tpu_forwards);
                        is_advertising_proxy = false;
                    }
                    return;
                }
            }
        }
    }

    fn set_tpu_addresses(
        cluster_info: &Arc<ClusterInfo>,
        tpu_address: SocketAddr,
        tpu_forward_address: SocketAddr,
    ) {
        let mut new_contact_info = cluster_info.my_contact_info();
        new_contact_info.tpu = tpu_address;
        new_contact_info.tpu_forwards = tpu_forward_address;
        cluster_info.set_my_contact_info(new_contact_info);
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
