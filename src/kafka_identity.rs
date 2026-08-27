use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use rdkafka::client::{Client, ClientContext};
use rdkafka_sys as sys;

pub fn cluster_id<C: ClientContext>(client: &Client<C>) -> Result<String> {
    client
        .fetch_cluster_id(Duration::from_secs(10))
        .context("broker did not return a cluster ID")
}

pub fn topic_id<C: ClientContext>(client: &Client<C>, topic: &str) -> Result<String> {
    topic_ids(client, &[topic])?
        .remove(topic)
        .with_context(|| format!("broker returned no identity for topic {topic}"))
}

pub fn topic_ids<C: ClientContext>(
    client: &Client<C>,
    topics: &[&str],
) -> Result<HashMap<String, String>> {
    if topics.is_empty() {
        return Ok(HashMap::new());
    }
    let topics: Vec<CString> = topics
        .iter()
        .map(|topic| CString::new(*topic).context("topic name contains a NUL byte"))
        .collect::<Result<_>>()?;
    let mut topic_ptrs: Vec<_> = topics.iter().map(|topic| topic.as_ptr()).collect();

    unsafe {
        let collection =
            sys::rd_kafka_TopicCollection_of_topic_names(topic_ptrs.as_mut_ptr(), topic_ptrs.len());
        if collection.is_null() {
            bail!("failed to allocate topic description request");
        }
        let options = sys::rd_kafka_AdminOptions_new(
            client.native_ptr(),
            sys::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBETOPICS,
        );
        let queue = sys::rd_kafka_queue_new(client.native_ptr());
        if options.is_null() || queue.is_null() {
            if !options.is_null() {
                sys::rd_kafka_AdminOptions_destroy(options);
            }
            if !queue.is_null() {
                sys::rd_kafka_queue_destroy(queue);
            }
            sys::rd_kafka_TopicCollection_destroy(collection);
            bail!("failed to allocate topic description request");
        }

        sys::rd_kafka_DescribeTopics(client.native_ptr(), collection, options, queue);
        sys::rd_kafka_AdminOptions_destroy(options);
        sys::rd_kafka_TopicCollection_destroy(collection);

        let event = sys::rd_kafka_queue_poll(queue, 10_000);
        sys::rd_kafka_queue_destroy(queue);
        if event.is_null() {
            bail!("timed out describing topic identity");
        }

        let result = (|| {
            let event_error = sys::rd_kafka_event_error(event);
            if event_error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let detail = c_string(sys::rd_kafka_event_error_string(event));
                bail!("topic identity request failed: {detail}");
            }
            let described = sys::rd_kafka_event_DescribeTopics_result(event);
            if described.is_null() {
                bail!("broker returned no topic identity result");
            }
            let mut count = 0usize;
            let descriptions = sys::rd_kafka_DescribeTopics_result_topics(described, &mut count);
            if descriptions.is_null() || count != topic_ptrs.len() {
                bail!(
                    "broker returned {count} topic descriptions instead of {}",
                    topic_ptrs.len()
                );
            }
            let mut identities = HashMap::with_capacity(count);
            for index in 0..count {
                let description = *descriptions.add(index);
                if description.is_null() {
                    bail!("broker returned an empty topic description");
                }
                let name = c_string(sys::rd_kafka_TopicDescription_name(description));
                let error = sys::rd_kafka_TopicDescription_error(description);
                if !error.is_null() {
                    bail!(
                        "failed to describe topic identity for {name}: {}",
                        c_string(sys::rd_kafka_error_string(error))
                    );
                }
                let uuid = sys::rd_kafka_TopicDescription_topic_id(description);
                if uuid.is_null()
                    || (sys::rd_kafka_Uuid_most_significant_bits(uuid) == 0
                        && sys::rd_kafka_Uuid_least_significant_bits(uuid) == 0)
                {
                    bail!(
                        "broker returned no topic UUID for {name}; Fransson requires Kafka 2.8 or newer"
                    );
                }
                let encoded = c_string(sys::rd_kafka_Uuid_base64str(uuid));
                if encoded.is_empty() {
                    return Err(anyhow!("broker returned an empty topic UUID for {name}"));
                }
                identities.insert(name, encoded);
            }
            Ok(identities)
        })();
        sys::rd_kafka_event_destroy(event);
        result
    }
}

unsafe fn c_string(value: *const std::os::raw::c_char) -> String {
    if value.is_null() {
        return "unknown error".to_owned();
    }
    CStr::from_ptr(value).to_string_lossy().into_owned()
}
