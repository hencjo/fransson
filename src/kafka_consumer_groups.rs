use std::ffi::{CStr, CString};
use std::ptr;

use anyhow::{bail, Context, Result};
use rdkafka::client::{Client, ClientContext};
use rdkafka_sys as sys;

const ADMIN_TIMEOUT_MS: i32 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffset {
    pub topic: String,
    pub partition: i32,
}

pub fn list_offsets<C: ClientContext>(
    client: &Client<C>,
    group: &str,
) -> Result<Vec<ConsumerGroupOffset>> {
    let group = CString::new(group).context("consumer group contains a NUL byte")?;
    unsafe {
        let request = sys::rd_kafka_ListConsumerGroupOffsets_new(group.as_ptr(), ptr::null());
        if request.is_null() {
            bail!("failed to allocate consumer group offset request");
        }
        let mut requests = [request];
        let queue = sys::rd_kafka_queue_new(client.native_ptr());
        if queue.is_null() {
            sys::rd_kafka_ListConsumerGroupOffsets_destroy(request);
            bail!("failed to allocate consumer group offset result queue");
        }
        sys::rd_kafka_ListConsumerGroupOffsets(
            client.native_ptr(),
            requests.as_mut_ptr(),
            requests.len(),
            ptr::null(),
            queue,
        );
        sys::rd_kafka_ListConsumerGroupOffsets_destroy(request);
        let event = sys::rd_kafka_queue_poll(queue, ADMIN_TIMEOUT_MS);
        sys::rd_kafka_queue_destroy(queue);
        if event.is_null() {
            bail!(
                "timed out listing offsets for consumer group {}",
                c_string(group.as_ptr())
            );
        }
        let result = parse_list_offsets_event(event, &c_string(group.as_ptr()));
        sys::rd_kafka_event_destroy(event);
        result
    }
}

pub fn delete_offsets<C: ClientContext>(
    client: &Client<C>,
    group: &str,
    offsets: &[ConsumerGroupOffset],
) -> Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let group = CString::new(group).context("consumer group contains a NUL byte")?;
    let topics: Vec<CString> = offsets
        .iter()
        .map(|offset| CString::new(offset.topic.as_str()).context("topic contains a NUL byte"))
        .collect::<Result<_>>()?;
    unsafe {
        let partitions = sys::rd_kafka_topic_partition_list_new(
            i32::try_from(offsets.len()).context("too many consumer offsets to delete")?,
        );
        if partitions.is_null() {
            bail!("failed to allocate consumer offset partition list");
        }
        for (offset, topic) in offsets.iter().zip(&topics) {
            let entry = sys::rd_kafka_topic_partition_list_add(
                partitions,
                topic.as_ptr(),
                offset.partition,
            );
            if entry.is_null() {
                sys::rd_kafka_topic_partition_list_destroy(partitions);
                bail!("failed to add consumer offset partition");
            }
        }
        let request = sys::rd_kafka_DeleteConsumerGroupOffsets_new(group.as_ptr(), partitions);
        sys::rd_kafka_topic_partition_list_destroy(partitions);
        if request.is_null() {
            bail!("failed to allocate consumer offset deletion request");
        }
        let mut requests = [request];
        let queue = sys::rd_kafka_queue_new(client.native_ptr());
        if queue.is_null() {
            sys::rd_kafka_DeleteConsumerGroupOffsets_destroy(request);
            bail!("failed to allocate consumer offset deletion result queue");
        }
        sys::rd_kafka_DeleteConsumerGroupOffsets(
            client.native_ptr(),
            requests.as_mut_ptr(),
            requests.len(),
            ptr::null(),
            queue,
        );
        sys::rd_kafka_DeleteConsumerGroupOffsets_destroy(request);
        let event = sys::rd_kafka_queue_poll(queue, ADMIN_TIMEOUT_MS);
        sys::rd_kafka_queue_destroy(queue);
        if event.is_null() {
            bail!(
                "timed out deleting offsets for consumer group {}",
                c_string(group.as_ptr())
            );
        }
        let result = parse_delete_offsets_event(event, &c_string(group.as_ptr()));
        sys::rd_kafka_event_destroy(event);
        result
    }
}

unsafe fn parse_list_offsets_event(
    event: *mut sys::rd_kafka_event_t,
    expected_group: &str,
) -> Result<Vec<ConsumerGroupOffset>> {
    ensure_event_success(event, "list consumer group offsets")?;
    let result = sys::rd_kafka_event_ListConsumerGroupOffsets_result(event);
    if result.is_null() {
        bail!("broker returned no consumer group offset result");
    }
    let mut count = 0usize;
    let groups = sys::rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut count);
    if groups.is_null() || count != 1 {
        bail!("broker returned {count} consumer group offset results instead of one");
    }
    let group = *groups;
    ensure_group_success(group, expected_group)?;
    let partitions = sys::rd_kafka_group_result_partitions(group);
    if partitions.is_null() {
        return Ok(Vec::new());
    }
    let mut offsets = Vec::new();
    for index in 0..(*partitions).cnt {
        let partition = &*(*partitions).elems.add(index as usize);
        if partition.err != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            bail!(
                "failed to list offset for {} partition {}: {:?}",
                c_string(partition.topic),
                partition.partition,
                partition.err
            );
        }
        if partition.offset >= 0 {
            offsets.push(ConsumerGroupOffset {
                topic: c_string(partition.topic),
                partition: partition.partition,
            });
        }
    }
    Ok(offsets)
}

unsafe fn parse_delete_offsets_event(
    event: *mut sys::rd_kafka_event_t,
    expected_group: &str,
) -> Result<()> {
    ensure_event_success(event, "delete consumer group offsets")?;
    let result = sys::rd_kafka_event_DeleteConsumerGroupOffsets_result(event);
    if result.is_null() {
        bail!("broker returned no consumer offset deletion result");
    }
    let mut count = 0usize;
    let groups = sys::rd_kafka_DeleteConsumerGroupOffsets_result_groups(result, &mut count);
    if groups.is_null() || count != 1 {
        bail!("broker returned {count} consumer offset deletion results instead of one");
    }
    let group = *groups;
    ensure_group_success(group, expected_group)?;
    let partitions = sys::rd_kafka_group_result_partitions(group);
    if partitions.is_null() {
        return Ok(());
    }
    for index in 0..(*partitions).cnt {
        let partition = &*(*partitions).elems.add(index as usize);
        if partition.err != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            bail!(
                "failed to delete offset for {} partition {}: {:?}",
                c_string(partition.topic),
                partition.partition,
                partition.err
            );
        }
    }
    Ok(())
}

unsafe fn ensure_event_success(event: *mut sys::rd_kafka_event_t, operation: &str) -> Result<()> {
    let error = sys::rd_kafka_event_error(event);
    if error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        bail!(
            "{operation} request failed: {}",
            c_string(sys::rd_kafka_event_error_string(event))
        );
    }
    Ok(())
}

unsafe fn ensure_group_success(
    group: *const sys::rd_kafka_group_result_t,
    expected_group: &str,
) -> Result<()> {
    if group.is_null() {
        bail!("broker returned an empty consumer group result");
    }
    let actual_group = c_string(sys::rd_kafka_group_result_name(group));
    if actual_group != expected_group {
        bail!("broker returned consumer group {actual_group} instead of {expected_group}");
    }
    let error = sys::rd_kafka_group_result_error(group);
    if !error.is_null() {
        bail!(
            "consumer group {expected_group} request failed: {}",
            c_string(sys::rd_kafka_error_string(error))
        );
    }
    Ok(())
}

unsafe fn c_string(value: *const std::os::raw::c_char) -> String {
    if value.is_null() {
        return "unknown".to_owned();
    }
    CStr::from_ptr(value).to_string_lossy().into_owned()
}
