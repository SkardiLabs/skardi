//! The `meta_whatsapp` provider preset: how Meta proves a request is its own,
//! and how its envelope is flattened into rows.
//!
//! Everything here follows Meta's webhook documentation for the WhatsApp
//! Business Platform:
//!
//! - Subscription handshake: a `GET` with `hub.mode=subscribe`,
//!   `hub.verify_token=<ours>` and `hub.challenge=<nonce>`; the endpoint
//!   answers 200 with the challenge verbatim as plain text.
//! - Delivery: a `POST` whose `X-Hub-Signature-256` header is
//!   `sha256=<hex HMAC-SHA256 of the raw body, keyed by the app secret>`.
//! - Envelope: `{object, entry: [{id: <WABA_ID>, changes: [{field, value}]}]}`,
//!   where `value` carries `metadata`, optionally `contacts`, and either a
//!   `messages` or a `statuses` array (both under `field: "messages"`).
//!
//! The unwrapper exists because the shared column converter walks object
//! keys only — no wildcards, no array indexes — by contract. Picking the
//! contact that matches a message, the media object named by the message
//! type, or the first error is therefore done here once, and exposed to the
//! column mapping as synthetic `_`-prefixed keys on each flat row.

use std::collections::HashMap;

use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use super::definition::RowSelector;

/// Header carrying the delivery signature.
pub const SIGNATURE_HEADER: &str = "x-hub-signature-256";

/// Message types whose payload object (named after the type) is a media
/// reference with `id`, `mime_type`, `sha256`, and optional `caption`.
const MEDIA_TYPES: &[&str] = &["image", "video", "audio", "document", "sticker"];

/// Why a handshake was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeError {
    /// `hub.mode` was absent or not `subscribe`.
    NotASubscribe,
    /// `hub.verify_token` was absent or did not match.
    TokenMismatch,
    /// `hub.challenge` was absent.
    MissingChallenge,
}

/// Answer the subscription handshake: the challenge to echo, or why not.
///
/// The token comparison is constant-time; the rest of the query is not
/// secret.
///
/// # Example
/// ```
/// use std::collections::HashMap;
/// use skardi_server::webhooks::meta_whatsapp::handshake;
///
/// let query: HashMap<String, String> = [
///     ("hub.mode", "subscribe"),
///     ("hub.verify_token", "expected"),
///     ("hub.challenge", "1158201444"),
/// ]
/// .into_iter()
/// .map(|(k, v)| (k.to_string(), v.to_string()))
/// .collect();
/// assert_eq!(handshake(&query, "expected").unwrap(), "1158201444");
/// assert!(handshake(&query, "other").is_err());
/// ```
pub fn handshake<'q>(
    query: &'q HashMap<String, String>,
    verify_token: &str,
) -> Result<&'q str, HandshakeError> {
    if query.get("hub.mode").map(String::as_str) != Some("subscribe") {
        return Err(HandshakeError::NotASubscribe);
    }
    let presented = query
        .get("hub.verify_token")
        .ok_or(HandshakeError::TokenMismatch)?;
    if presented
        .as_bytes()
        .ct_eq(verify_token.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(HandshakeError::TokenMismatch);
    }
    query
        .get("hub.challenge")
        .map(String::as_str)
        .ok_or(HandshakeError::MissingChallenge)
}

/// Why a delivery signature was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureError {
    /// No `X-Hub-Signature-256` header.
    Missing,
    /// The header was not `sha256=<hex>`.
    Malformed,
    /// The MAC did not match the body.
    Mismatch,
}

/// Verify `X-Hub-Signature-256` over the raw body bytes.
///
/// The body must be the bytes exactly as received: any re-serialization
/// (pretty-printing, key reordering) changes the MAC.
///
/// # Example
/// ```
/// use hmac::{Hmac, Mac};
/// use sha2::Sha256;
/// use skardi_server::webhooks::meta_whatsapp::verify_signature;
///
/// let body = br#"{"object":"whatsapp_business_account","entry":[]}"#;
/// let mut mac = Hmac::<Sha256>::new_from_slice(b"app-secret").unwrap();
/// mac.update(body);
/// let header = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
///
/// assert!(verify_signature(Some(&header), body, "app-secret").is_ok());
/// assert!(verify_signature(Some(&header), body, "wrong").is_err());
/// assert!(verify_signature(None, body, "app-secret").is_err());
/// ```
pub fn verify_signature(
    header: Option<&str>,
    body: &[u8],
    app_secret: &str,
) -> Result<(), SignatureError> {
    let header = header.ok_or(SignatureError::Missing)?;
    let hex_mac = header
        .strip_prefix("sha256=")
        .ok_or(SignatureError::Malformed)?;
    let presented = hex::decode(hex_mac.trim()).map_err(|_| SignatureError::Malformed)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(app_secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(body);
    mac.verify_slice(&presented)
        .map_err(|_| SignatureError::Mismatch)
}

/// The `object` value every WhatsApp Business Platform delivery carries.
pub const WHATSAPP_OBJECT: &str = "whatsapp_business_account";

/// Why an envelope could not be unwrapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnwrapError {
    /// The body is not a JSON object with an `entry` array.
    NotAnEnvelope,
    /// The envelope belongs to another Meta product (`page`, `instagram`,
    /// …) that shares the app's callback URL and app secret.
    WrongObject { found: String },
}

/// Flatten one delivery into the rows a route selects.
///
/// For [`RowSelector::List`], every change whose `value.<field>` is an array
/// contributes one row per element; for [`RowSelector::Value`], every change
/// whose `field` label equals `field` contributes its `value` as one row.
/// Entries and changes that do not match are skipped, never an error — one
/// delivery routinely carries fields the receiver has no route for.
///
/// Each row is the original object plus synthetic keys:
///
/// | key | value |
/// |---|---|
/// | `_waba_id` | `entry.id` |
/// | `_metadata` | `value.metadata` (list rows) |
/// | `_contact` | the `value.contacts[]` entry whose `wa_id` equals the row's `from` (or `recipient_id`) |
/// | `_media` | the object named by the row's `type`, for media types |
/// | `_interactive_reply` | `interactive.button_reply` or `interactive.list_reply` |
/// | `_error` | `errors[0]` |
/// | `_raw` | the original object, unchanged |
///
/// # Example
/// ```
/// use serde_json::json;
/// use skardi_server::webhooks::{RowSelector, meta_whatsapp::unwrap_rows};
///
/// let delivery = json!({
///   "object": "whatsapp_business_account",
///   "entry": [{"id": "WABA1", "changes": [{"field": "messages", "value": {
///     "metadata": {"phone_number_id": "PN1"},
///     "contacts": [{"wa_id": "16505551234", "profile": {"name": "Sheena"}}],
///     "messages": [{"from": "16505551234", "id": "wamid.1", "type": "text",
///                   "timestamp": "1749416383", "text": {"body": "hi"}}]
///   }}]}]
/// });
/// let rows = unwrap_rows(&delivery, "messages", RowSelector::List).unwrap();
/// assert_eq!(rows.len(), 1);
/// assert_eq!(rows[0]["_waba_id"], "WABA1");
/// assert_eq!(rows[0]["_contact"]["profile"]["name"], "Sheena");
/// assert_eq!(rows[0]["_raw"]["text"]["body"], "hi");
/// assert!(unwrap_rows(&delivery, "statuses", RowSelector::List).unwrap().is_empty());
/// ```
pub fn unwrap_rows(
    envelope: &Value,
    field: &str,
    selector: RowSelector,
) -> Result<Vec<Value>, UnwrapError> {
    let root = envelope.as_object().ok_or(UnwrapError::NotAnEnvelope)?;
    // One Meta app can point several products' webhooks at the same URL,
    // all signed with the same secret; only WhatsApp's envelope is ours.
    match root.get("object").and_then(Value::as_str) {
        Some(WHATSAPP_OBJECT) => {}
        Some(other) => {
            return Err(UnwrapError::WrongObject {
                found: other.to_string(),
            });
        }
        None => return Err(UnwrapError::NotAnEnvelope),
    }
    let entries = root
        .get("entry")
        .and_then(Value::as_array)
        .ok_or(UnwrapError::NotAnEnvelope)?;

    let mut rows = Vec::new();
    for entry in entries {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        let waba_id = entry.get("id").cloned().unwrap_or(Value::Null);
        let Some(changes) = entry.get("changes").and_then(Value::as_array) else {
            continue;
        };
        for change in changes {
            let Some(change) = change.as_object() else {
                continue;
            };
            let Some(value) = change.get("value").and_then(Value::as_object) else {
                continue;
            };
            match selector {
                RowSelector::List => {
                    let Some(items) = value.get(field).and_then(Value::as_array) else {
                        continue;
                    };
                    for item in items {
                        if let Some(item) = item.as_object() {
                            rows.push(flatten_list_row(item, value, &waba_id));
                        }
                    }
                }
                RowSelector::Value => {
                    if change.get("field").and_then(Value::as_str) != Some(field) {
                        continue;
                    }
                    let mut row = value.clone();
                    row.insert("_waba_id".to_string(), waba_id.clone());
                    row.insert("_raw".to_string(), Value::Object(value.clone()));
                    rows.push(Value::Object(row));
                }
            }
        }
    }
    Ok(rows)
}

fn flatten_list_row(
    item: &Map<String, Value>,
    value: &Map<String, Value>,
    waba_id: &Value,
) -> Value {
    let mut row = item.clone();
    row.insert("_waba_id".to_string(), waba_id.clone());
    row.insert(
        "_metadata".to_string(),
        value.get("metadata").cloned().unwrap_or(Value::Null),
    );

    // A message names its sender in `from`; a status names the customer in
    // `recipient_id`. Either is the `wa_id` the contacts array is keyed by.
    let counterpart = item
        .get("from")
        .or_else(|| item.get("recipient_id"))
        .and_then(Value::as_str);
    let contact = counterpart
        .and_then(|wa_id| {
            value
                .get("contacts")
                .and_then(Value::as_array)?
                .iter()
                .find(|c| c.get("wa_id").and_then(Value::as_str) == Some(wa_id))
        })
        .cloned()
        .unwrap_or(Value::Null);
    row.insert("_contact".to_string(), contact);

    let media = item
        .get("type")
        .and_then(Value::as_str)
        .filter(|t| MEDIA_TYPES.contains(t))
        .and_then(|t| item.get(t))
        .cloned()
        .unwrap_or(Value::Null);
    row.insert("_media".to_string(), media);

    let interactive_reply = item
        .get("interactive")
        .and_then(Value::as_object)
        .and_then(|i| i.get("button_reply").or_else(|| i.get("list_reply")))
        .cloned()
        .unwrap_or(Value::Null);
    row.insert("_interactive_reply".to_string(), interactive_reply);

    let error = item
        .get("errors")
        .and_then(Value::as_array)
        .and_then(|e| e.first())
        .cloned()
        .unwrap_or(Value::Null);
    row.insert("_error".to_string(), error);

    row.insert("_raw".to_string(), Value::Object(item.clone()));
    Value::Object(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn fixture(name: &str) -> Value {
        let path = format!(
            "{}/tests/fixtures/webhooks/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&text).expect("fixture is JSON")
    }

    fn query(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn handshake_echoes_the_challenge_only_for_a_matching_token() {
        let q = query(&[
            ("hub.mode", "subscribe"),
            ("hub.verify_token", "tok"),
            ("hub.challenge", "1158201444"),
        ]);
        assert_eq!(handshake(&q, "tok"), Ok("1158201444"));
        assert_eq!(handshake(&q, "tok2"), Err(HandshakeError::TokenMismatch));
        let q = query(&[("hub.mode", "unsubscribe"), ("hub.verify_token", "tok")]);
        assert_eq!(handshake(&q, "tok"), Err(HandshakeError::NotASubscribe));
        let q = query(&[("hub.mode", "subscribe"), ("hub.verify_token", "tok")]);
        assert_eq!(handshake(&q, "tok"), Err(HandshakeError::MissingChallenge));
        let q = query(&[("hub.mode", "subscribe"), ("hub.challenge", "x")]);
        assert_eq!(handshake(&q, "tok"), Err(HandshakeError::TokenMismatch));
    }

    #[test]
    fn signature_round_trips_and_rejects_every_other_shape() {
        let body = br#"{"object":"whatsapp_business_account","entry":[]}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(b"s3cret").unwrap();
        mac.update(body);
        let good = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert_eq!(verify_signature(Some(&good), body, "s3cret"), Ok(()));
        assert_eq!(
            verify_signature(Some(&good), body, "other"),
            Err(SignatureError::Mismatch)
        );
        assert_eq!(
            verify_signature(Some(&good), b"{\"tampered\":true}", "s3cret"),
            Err(SignatureError::Mismatch)
        );
        assert_eq!(
            verify_signature(None, body, "s3cret"),
            Err(SignatureError::Missing)
        );
        assert_eq!(
            verify_signature(Some("sha1=abcd"), body, "s3cret"),
            Err(SignatureError::Malformed)
        );
        assert_eq!(
            verify_signature(Some("sha256=zz"), body, "s3cret"),
            Err(SignatureError::Malformed)
        );
    }

    #[test]
    fn text_message_gets_contact_metadata_and_raw() {
        let rows = unwrap_rows(&fixture("text_message"), "messages", RowSelector::List).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row["_waba_id"], "102290129340398");
        assert_eq!(row["_metadata"]["phone_number_id"], "106540352242922");
        assert_eq!(row["_metadata"]["display_phone_number"], "15550783881");
        assert_eq!(row["_contact"]["profile"]["name"], "Sheena Nelson");
        assert_eq!(row["text"]["body"], "Does it come in another color?");
        assert_eq!(row["_media"], Value::Null);
        assert_eq!(row["_interactive_reply"], Value::Null);
        assert_eq!(row["_error"], Value::Null);
        assert_eq!(row["_raw"]["id"], row["id"]);
        assert!(
            row["_raw"].get("_waba_id").is_none(),
            "_raw is the untouched object"
        );
        // The same delivery feeds no status rows.
        assert!(
            unwrap_rows(&fixture("text_message"), "statuses", RowSelector::List)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn image_message_exposes_the_media_object_named_by_its_type() {
        let rows = unwrap_rows(&fixture("image_message"), "messages", RowSelector::List).unwrap();
        assert_eq!(rows[0]["_media"]["id"], "1003383421387256");
        assert_eq!(rows[0]["_media"]["mime_type"], "image/jpeg");
        assert_eq!(rows[0]["_media"]["caption"], "Taj Mahal");
    }

    #[rstest]
    #[case::list_reply("interactive_list_reply", "priority_express")]
    #[case::button_reply("interactive_button_reply", "cancel-button")]
    fn interactive_replies_expose_the_reply_and_the_context(
        #[case] name: &str,
        #[case] reply_id: &str,
    ) {
        let rows = unwrap_rows(&fixture(name), "messages", RowSelector::List).unwrap();
        assert_eq!(rows[0]["_interactive_reply"]["id"], reply_id);
        assert!(
            rows[0]["context"]["id"]
                .as_str()
                .unwrap()
                .starts_with("wamid.")
        );
    }

    #[test]
    fn unsupported_message_exposes_its_first_error() {
        let rows = unwrap_rows(
            &fixture("unsupported_message"),
            "messages",
            RowSelector::List,
        )
        .unwrap();
        assert_eq!(rows[0]["type"], "unsupported");
        assert_eq!(rows[0]["_error"]["code"], 131051);
    }

    #[rstest]
    #[case::delivered("status_delivered", "delivered", Some("service"), None)]
    #[case::sent_pmp("status_sent_pmp", "sent", Some("marketing"), None)]
    #[case::failed("status_failed", "failed", None, Some(131049))]
    fn statuses_are_list_rows_under_the_messages_field(
        #[case] name: &str,
        #[case] status: &str,
        #[case] category: Option<&str>,
        #[case] error_code: Option<i64>,
    ) {
        let delivery = fixture(name);
        // The change's own field label is "messages" for both arrays.
        assert_eq!(delivery["entry"][0]["changes"][0]["field"], "messages");
        let rows = unwrap_rows(&delivery, "statuses", RowSelector::List).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["status"], status);
        assert_eq!(rows[0]["_waba_id"], "102290129340398");
        assert_eq!(rows[0]["_metadata"]["phone_number_id"], "106540352242922");
        match category {
            Some(c) => assert_eq!(rows[0]["pricing"]["category"], c),
            None => assert!(rows[0].get("pricing").is_none()),
        }
        match error_code {
            Some(code) => assert_eq!(rows[0]["_error"]["code"], code),
            None => assert_eq!(rows[0]["_error"], Value::Null),
        }
        assert!(
            unwrap_rows(&delivery, "messages", RowSelector::List)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn value_rows_match_the_change_field_label() {
        let delivery = serde_json::json!({
            "object": "whatsapp_business_account",
            "object": "whatsapp_business_account",
            "entry": [{"id": "W1", "changes": [
                {"field": "message_template_status_update",
                 "value": {"event": "APPROVED", "message_template_id": 123, "message_template_name": "hello"}},
                {"field": "messages", "value": {"messages": [{"id": "m", "from": "1", "type": "text"}]}}
            ]}]
        });
        let rows = unwrap_rows(
            &delivery,
            "message_template_status_update",
            RowSelector::Value,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["event"], "APPROVED");
        assert_eq!(rows[0]["_waba_id"], "W1");
        assert_eq!(rows[0]["_raw"]["message_template_name"], "hello");
        // A list route for the same label name reads value.<label>[], which this change lacks.
        assert!(
            unwrap_rows(
                &delivery,
                "message_template_status_update",
                RowSelector::List
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn a_batched_delivery_yields_every_row_across_entries_and_changes() {
        let delivery = serde_json::json!({
            "object": "whatsapp_business_account",
            "entry": [
                {"id": "W1", "changes": [
                    {"field": "messages", "value": {"messages": [{"id": "a", "from": "1", "type": "text"}, {"id": "b", "from": "2", "type": "text"}]}},
                    {"field": "messages", "value": {"statuses": [{"id": "a", "status": "sent", "recipient_id": "9"}]}}
                ]},
                {"id": "W2", "changes": [
                    {"field": "messages", "value": {"messages": [{"id": "c", "from": "3", "type": "text"}]}}
                ]}
            ]
        });
        let rows = unwrap_rows(&delivery, "messages", RowSelector::List).unwrap();
        let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["a", "b", "c"]);
        assert_eq!(rows[2]["_waba_id"], "W2");
        assert_eq!(
            unwrap_rows(&delivery, "statuses", RowSelector::List)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn a_body_without_an_entry_array_is_not_an_envelope() {
        for body in [
            serde_json::json!({"object": "whatsapp_business_account"}),
            serde_json::json!({"object": "whatsapp_business_account", "entry": {}}),
            serde_json::json!({"entry": []}),
            serde_json::json!([]),
            serde_json::json!("entry"),
        ] {
            assert_eq!(
                unwrap_rows(&body, "messages", RowSelector::List),
                Err(UnwrapError::NotAnEnvelope)
            );
        }
        // Malformed entries inside a real envelope are skipped, not fatal.
        let body = serde_json::json!({"object": "whatsapp_business_account",
                                      "entry": [1, {"id": "W", "changes": [2, {"value": "x"}]}]});
        assert_eq!(
            unwrap_rows(&body, "messages", RowSelector::List),
            Ok(vec![])
        );
    }

    #[test]
    fn another_products_envelope_is_refused_by_its_object() {
        let body = serde_json::json!({"object": "page", "entry": [{"id": "1", "changes": []}]});
        assert_eq!(
            unwrap_rows(&body, "messages", RowSelector::List),
            Err(UnwrapError::WrongObject {
                found: "page".to_string()
            })
        );
    }
}
